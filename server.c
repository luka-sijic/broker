#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define RESPONSE "HTTP/1 200 OK"
#define EVBUF_SIZE 64

/*
API
PRODUCE == 1
CONSUME == 0


*/

typedef struct {
  uint8_t type;
  uint32_t id;
  // timestamp?
} broker;

typedef struct {
  int client_fd;
  char *username;
} user;

static int write_all(int fd, const void *buf, size_t len) {
  const char *p = (const char *)buf;
  while (len) {
    ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
    if (n > 0) {
      p += n;
      len -= (size_t)n;
      continue;
    }
    if (n == -1 && (errno == EINTR))
      continue;
    if (n == -1 &&
        (errno == EAGAIN || errno == EWOULDBLOCK)) { /* wait in your loop */
      continue;
    }
    return -1; // real error
  }
  return 0;
}

void parse_req(broker *b, unsigned char *req) {
  char *save = NULL;
  for (char *tok = strtok_r((char *)req, " \t\r\n", &save); tok;
       tok = strtok_r(NULL, " \t\r\n", &save)) {
    if (strcmp(tok, "1"))
      b->type = 1;
    else if (strcmp(tok, "0"))
      b->type = 0;
    else
      b->id = (uint32_t)tok;
    puts(tok);
  }
}

static int set_nonblocking(int fd) {
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl < 0)
    return -1;
  return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static void add_event(int kq, int fd, short filter, uint16_t flags) {
  struct kevent ev;
  EV_SET(&ev, fd, filter, flags, 0, 0, NULL);
  if (kevent(kq, &ev, 1, NULL, 0, NULL) < 0) {
    perror("kevent add");
    exit(1);
  }
}

static void del_event_quiet(int kq, int fd, short filter) {
  struct kevent ev;
  EV_SET(&ev, fd, filter, EV_DELETE, 0, 0, NULL);
  (void)kevent(kq, &ev, 1, NULL, 0, NULL);
}

static void respond_text(int fd, int status, const char *reason,
                         const char *body, int close_conn) {
  char hdr[512];
  size_t blen = body ? strlen(body) : 0;
  int n = snprintf(hdr, sizeof hdr,
                   "HTTP/1.1 %d %s\r\n"
                   "Content-Type: text/plain; charset=utf-8\r\n"
                   "Content-Length: %zu\r\n"
                   "Connection: %s\r\n"
                   "\r\n",
                   status, reason, blen, close_conn ? "close" : "keep-alive");
  write_all(fd, hdr, (size_t)n);
  if (blen)
    write_all(fd, body, blen);
}

void handle_client(int kq, int client_fd) {
  unsigned char buffer[4096];

  for (;;) {
    ssize_t nread = recv(client_fd, buffer, sizeof buffer, 0);
    if (nread > 0) {
      buffer[nread] = '\0';
      broker *b = malloc(sizeof(broker));
      parse_req(b, buffer);
      printf("ID: %d", b->id);
      // respond_text(client_fd, 200, "OK", "HELLO FROM C\n", 1);
      //  send(client_fd, RESPONSE, sizeof RESPONSE, 0);
    } else {
      break;
    }
    // fwrite(buffer, 1, (size_t)nread, stdout);
  }
  del_event_quiet(kq, client_fd, EVFILT_READ);
  close(client_fd);
}

int main(void) {
  int opt = 1;
  struct sockaddr_in server_addr;
  int server_fd = socket(PF_INET, SOCK_STREAM, 0);
  memset(&server_addr, '\0', sizeof(server_addr));

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(7005);
  server_addr.sin_addr.s_addr = INADDR_ANY;

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
    perror("sock opt failed");
  }

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
      0) {
    perror("bind failed");
  }
  if (set_nonblocking(server_fd) < 0) {
    perror("fcntl");
    return 1;
  }

  if (listen(server_fd, SOMAXCONN) < 0) {
    perror("listen failed");
  }

  int kq = kqueue();
  if (kq < 0) {
    perror("kqueue");
    return 1;
  }

  add_event(kq, server_fd, EVFILT_READ, EV_ADD | EV_ENABLE);
  struct kevent evs[EVBUF_SIZE];

  for (;;) {
    int nev = kevent(kq, NULL, 0, evs, EVBUF_SIZE, NULL);
    if (nev < 0) {
      if (errno == EINTR)
        continue;
      perror("kevent wait");
      break;
    }

    for (int i = 0; i < nev; i++) {
      int fd = (int)evs[i].ident;
      if (fd == server_fd && evs[i].filter == EVFILT_READ) {
        for (;;) {
          struct sockaddr_in cli;
          socklen_t cl = sizeof(cli);
          int cfd = accept(server_fd, (struct sockaddr *)&cli, &cl);
          if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
              break;
            if (errno == EINTR)
              continue;
            perror("accept");
            break;
          }
          if (set_nonblocking(cfd) < 0) {
            perror("fcntl client");
            close(cfd);
            continue;
          }
          add_event(kq, cfd, EVFILT_READ, EV_ADD | EV_ENABLE);
        }
        continue;
      }

      if (evs[i].filter == EVFILT_READ && fd != server_fd) {
        handle_client(kq, fd);
      }
    }
  }
  close(server_fd);
  close(kq);
  return 0;
}

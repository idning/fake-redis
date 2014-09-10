#!/usr/bin/env python
#coding: utf-8
#file   : fack-redis.py
#author : ning
#date   : 2014-09-09 17:36:17

import os
import re
import sys
import time
import copy
import thread
import logging
import SocketServer

class ProtocolException(Exception):
    pass

store = {}

SLOW_SEND = True

class RedisHandler(SocketServer.BaseRequestHandler):
    def send(self, content):
        if SLOW_SEND:
            for i in content:
                self.request.sendall(i)
                time.sleep(.15)
        else:
            self.request.sendall(content)

    def reply_bulk(self, content):
        if content == None:
            self.send('$-1\r\n')
        else:
            self.send('$%d\r\n' % len(content))
            self.send(content)
            self.send('\r\n')

    def reply_ok(self):
        self.send('+OK\r\n')

    def reply_err(self):
        self.send('-ERR\r\n')

    def reply_mbulk(self, arr):
        self.send('*%d\r\n' % len(arr))
        for content in arr:
            self.send('$%d\r\n' % len(content))
            self.send(content)
            self.send('\r\n')

    def handle_get(self, argv):
        key = argv[1]
        if key == 'special-key-for-hhvm':

            # $10\r\n$1234567\r\n
            self.request.sendall('$1')
            time.sleep(.15)
            self.request.sendall('0\r\n$123456789\r\n')

        elif key in store:
            self.reply_bulk(store[key])
        else:
            self.reply_bulk(None)

    def handle_set(self, argv):
        print 'handle_set'
        key = argv[1]
        val = argv[2]
        store[key] = val

        self.reply_ok()

    def handle_hmset(self, argv):
        key = argv[1]
        if key not in store:
            store[key] = {}
        if type(store[key]) != dict:
            store[key] = {}
        for i in range((len(argv) - 2) / 2):
            fld = argv[2+i*2]
            val = argv[3+i*2]
            store[key][fld] = val

        self.reply_ok()

    def handle_hmget(self, argv):
        key = argv[1]
        arr = []

        for i in range((len(argv) - 2)):
            fld = argv[2+i]
            arr.append(fld)
            arr.append(store[key][fld])

        self.reply_mbulk(arr)

    def handle_hgetall(self, argv):
        key = argv[1]
        arr = []

        for fld, val in store[key].items():
            arr.append(fld)
            arr.append(val)

        self.reply_mbulk(arr)

    def handle(self):
        self.cmdmap = {
            'get': self.handle_get,
            'set': self.handle_set,
            'hmset': self.handle_hmset,
            'hmget': self.handle_hmget,
            'hgetall': self.handle_hgetall,
        }
        def read_line():
            line = ''
            while True:
                c = self.request.recv(1)
                line += c
                if c == '\n':
                    return line

        def read_int(mark):
            line = read_line()
            if line[0] != mark:
                raise ProtocolException()
            return int(line[1:])

        def read_bytes(size):
            content = ''
            while len(content) < size:
                t = self.request.recv(size - len(content))
                content += t

            return content

        while True:
            argc = read_int('*')
            argv = []
            for i in range(argc):
                arglen = read_int('$')
                arg = read_bytes(arglen + 2)
                arg = arg[:-2]
                print 'getarg', arg
                argv.append(arg)

            print 'processing', argv
            cmd = argv[0].lower()
            processer = self.cmdmap[cmd]
            processer(argv)

class FackRedis(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, host, port):
        self.allow_reuse_address = True

        SocketServer.TCPServer.__init__(self, (host, port), RedisHandler)
        self.store = {}

    def run(self):
        self.serve_forever()

if __name__ == "__main__":
    HOST, PORT = "localhost", 9999

    FackRedis(HOST, PORT).run()


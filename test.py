#!/usr/bin/env python3.6
import asyncio
import aiohttp
import json
import sys
import logging
import time
import re

# Goloman talks with Hands, Holiday and Wilkes.
# Hands talks with Wilkes.
# Holiday talks with Welsh and Wilkes.
# IAMAT golden +34.068930-118.445127 1520023934.918963997
url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=51.507351,-0.127758&radius=1000&types=restaurant&key="
key = "AIzaSyDFr6tmIPuRMgH4t2_6b5_E8gHTQvNFxIU"
servers = {'Goloman': 12558, 'Hands': 12559, 'Holiday': 12560, 'Welsh': 12561, 'Wilkes': 12562}
talks = {
    'Goloman': ['Hands', 'Holiday', 'Wilkes'],
    'Hands': ['Wilkes', 'Goloman'],
    'Holiday': ['Welsh', 'Wilkes', 'Goloman'],
    'Welsh': ['Holiday'],
    'Wilkes': ['Goloman', 'Hands', 'Holiday']
}
clients = {}
# {id, [location, time_difference, og_time]}
log = ""
file = ""


class ServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        file.write('Connection from {} \n'.format(peername))
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        message = message.strip()
        splited = message.split(' ')
        # print(splited)
        print('Data received: {!r}'.format(message))
        file.write('Data received: {!r}\n'.format(message))

        if splited[0] == "IAMAT":
            location = list(splited[2])
            isPOSIX = True
            if (location[0] != '+' and location[0] != '-') or ('-' not in location[1:] and '+' not in location[1:]):
                isPOSIX = False

            if len(splited) != 4 or not isPOSIX or not splited[3].replace('.', '', 1).isdigit() or re.search('[a-zA-Z]', splited[2]) or re.search('[a-zA-Z]', splited[3]):
            #IAMAT kiwi.cs.ucla.edu +34.068930-118.445127 1520023934.91896399
                print("Invalid IAMAT format!")
                file.write("Invalid IAMAT format!\n")
                self.transport.write(("? " + message + '\n').encode())
                file.write("Data sent to client: ? " + message + '\n')
            else:
                asyncio.ensure_future(self.handleIAMAT(splited))

        elif splited[0] == "WHATSAT":
            if len(splited) != 4 or splited[1] not in clients or int(splited[2]) > 50 or int(splited[3]) > 20:
            #WHATSAT kiwi.cs.ucla.edu 10 0
                print("Invalid WHATSAT format!")
                file.write("Invalid WHATSAT format!\n")
                self.transport.write(("? " + message + '\n').encode())
                file.write("Data sent to client: ? " + message + '\n')
            else:
                print("Getting Json from Google Places")
                file.write("Getting Json from Google Places\n")
                asyncio.ensure_future(self.handleWHATSAT(splited))

        elif splited[0] == "AT":
            asyncio.ensure_future(self.handleAT(splited))
        else:
            self.transport.write(("? " + message + '\n').encode())
            # file.write("Data sent to client: ? " + message + '\n')

    async def handleIAMAT(self, input):
        time_diff = time.time() - float(input[3])
        if time_diff > 0:
            time_string = "+" + str(time_diff)
        else:
            time_string = str(time_diff)

        #if the time is less than the time stored, then respond when the most recent time and loaction
        older = False
        if input[1] in clients:
            if float(input[3]) <= float(clients[input[1]][2]):
                print("timestamp is older or the same then before, do not propagate")
                file.write("timestamp is older or the same then before, do not propagate")
                older = True
                # time_string = str(clients[input[1]][1])

        response = ("AT" + " " + current_server + " " + time_string + " " + input[1] + " " + input[2] + " " + input[3] + "\n")
        clients[input[1]] = [input[2], time_string, input[3]]
        # print(clients)
        print("Data sent to client: " + response)
        file.write("Data sent to client: " + response + '\n')
        self.transport.write(response.encode())

        # flood AT message
        if not older:
            for server in talks[current_server]:
                asyncio.ensure_future(flood(response, server))

    async def handleAT(self, input):
        # message = message.strip()
        # input = message.split(' ')
        id = input[-3]
        time_diff = input[-4]
        location = input[-2]
        og_time = input[-1]
        input.insert(1, current_server)
        message = " ".join(input)

# AT Wilkes Holiday +8313464.08937192 kiwi.cs.ucla.edu +34.068930-118.445127 1520023934.918963997'
        if id in clients:
            if og_time <= clients[id][2]:
                print("Already reveived this information or the timestamp is newer")
                file.write("Already received this information or the timestamp is newer\n")
                return
            else:
                clients[id][0] = location
                clients[id][1] = time_diff
                clients[id][2] = og_time
                print("Updated client information: " + str(clients[id]))
                file.write("Updated client information: " + str(clients[id]) + '\n')
        else:
            print("New client created: " + id)
            file.write("New client created: " + id + '\n')
            clients[id] = [location, time_diff, og_time]
            #flood out new information
        for server in talks[current_server]:
            if server not in message:
                asyncio.ensure_future(flood(message, server))
                print("Try forwarding AT message to: " + server + ": " + str(servers[server]))
                file.write("Try forwarding AT message to: " + server + ": " + str(servers[server]) + '\n')

    async def handleWHATSAT(self, input):
        # WHATSAT kiwi.cs.ucla.edu 10 5
        id = input[1]
        radius = input[2]
        information = int(input[3])
        location = clients[id][0]
        #format the location
        location = list(location)
        if '+' in location[1:]:
            location.insert(location[1:].index('+')+1, ',')
        else:
            location.insert(location[1:].index('-')+1, ',')
        location = ''.join(location)

        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=%s&radius=%s&key=AIzaSyDFr6tmIPuRMgH4t2_6b5_E8gHTQvNFxIU' % (location, radius))
            data = json.loads(html)
            data['results'] = data['results'][:information]
            jsondata = json.dumps(data, indent = 3) + "\n\n"

            at_message = ("AT" + " " + current_server + " " + clients[input[1]][1] + " " + input[1] + " " + input[2] + " " + input[3] + "\n")

            response = at_message + jsondata
            self.transport.write(response.encode())

    async def fetch(self, session, url):
            async with session.get(url) as response:
                return await response.text()

async def flood(response, server):
    try:
    # await asyncio.create_connection('127.0.0.1', port)
        loop = asyncio.get_event_loop()
        coro = await loop.create_connection(lambda: Client(loop, response, server), '127.0.0.1', servers[server])
        print("Successfully flooded message to %s, %d" % (server, servers[server]))
        file.write("Successfully flooded message to %s, %d \n" % (server, servers[server]))
    # asyncio.ensure_future(coro)
    except:
        print("Failed to flood message to %s, %d" % (server, servers[server]))
        file.write("Failed to flood message to %s, %d\n" % (server, servers[server]))

async def connect_servers(response, server):
    try:
        loop = asyncio.get_event_loop()
        coro = await loop.create_connection(lambda: Client(loop, response, server), '127.0.0.1', servers[server])
        print("Connected to " + server)
        file.write("Connected to " + server + '\n')
    except:
        # print("Failed to connect to %s, %d" % (server, servers[server]))
        pass


class Client(asyncio.Protocol):
    def __init__(self, loop, message, target_server):
        self.message = message
        self.loop = loop
        self.target_server = target_server

    def connection_made(self, transport):
        message = self.message
        transport.write(message.encode())
        # print('Data flooded to ' + self.target_server + ': {!r}'.format(message))

    # def data_received(self, data):
    #     print('Data received: {!r}'.format(data.decode()))

    def connection_lost(self, exc):
        print('The server closed the connection: ' +  self.target_server)

def main():
    if len(sys.argv) != 2:
        print("Number of arguments invalid!")
        exit(1)

    if sys.argv[1] not in servers:
        print("Invalid server name!")
        exit(1)
    else:
        port = servers[sys.argv[1]]
        global current_server
        current_server = sys.argv[1]

    global log
    log = current_server + ".log"
    global file
    file = open(log, 'a+')

    loop = asyncio.get_event_loop()
    coro = loop.create_server(ServerProtocol, '127.0.0.1', port)
    # coro = asyncio.start_server(ServerProtocol, '127.0.0.1', port, loop = loop)
    server = loop.run_until_complete(coro)

    print('Serving on {}'.format(server.sockets[0].getsockname()))

    # simply send a message notifying servers it talks to that its up
    # for serverrr in talks[current_server]:
    #     message = current_server + " is up"
    #     asyncio.ensure_future(connect_servers(message, serverrr))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Closing " + current_server)
        file.write("Closing " + current_server + '\n')

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':
    main()

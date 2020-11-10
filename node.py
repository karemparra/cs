import zmq
import sys
import random

from hashlib import sha256

class Server:
    def __init__ (self, ip, succesor):
        self.ip = str(ip)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.connection = self.socket.bind("tcp://*:"+str(ip))
        self.id = 0
        self.intResp = [0, 20]
        self.pred = None
        self.succesor = succesor

    def run(self):
        if self.succesor == "genesis":
            self.pred = self.ip
            self.succesor = self.ip
            print(f"Genesis node, has created with ip {self.ip}, 
            succesor {self.succesor} and predecessor {self.pred}")
        else:
            self.server_connection()
        self.listen()

    def listen(self):
        res = self.socket.recv_multipart()
        cmd = res[0].decode('utf-8')

        if cmd == 'connect':
            ipClient = res[1].decode('utf-8')
            idClient = res[2].decode('utf-8')
            if in_interval(idClient):
                self.socket.send_multipart([b'in_interval', self.succesor.encode('utf-8')])

                self.socket = self.context.socket(zmq.REQ)
                self.connection = self.socket.connect("tcp://localhost:"+self.succesor)
                self.socket.send_multipart([b'change_pred', ipClient.encode('utf-8')])
                self.socket.recv_multipart()

                self.socket = self.context.socket(zmq.REP)
                self.connection = self.socket.bind("tcp://*:"+self.ip)
                self.succesor = ipClient
                
        
        elif cmd == 'change_pred':
            newPred = res[1].decode('utf-8')
            self.pred = newPred
            self.socket.send_multipart([b'changed predecessor'])

    def in_interval(self, id):
            return True

    def id_assignment(self):
        numRandom = str(random.randrange(5000)).encode('utf-8')
        #CAMBIAR EN 16, 5000 POR STRING DE 40 CARACT
        hashID = sha256()
        hashID.update(self.ip.encode('utf-8'))
        hashID.update(numRandom)
        self.id = int(hashID.hexdigest(),16)
        return self.id  #ELIMINAR DESPUÉS
    
    def server_connection(self, ipGenesis = 0):
        while True:
            self.socket = self.context.socket(zmq.REQ)
            self.connection = self.socket.connect("tcp://localhost:"+self.succesor)
            self.socket.send_multipart([b'connect', self.ip.encode('utf-8'), self.id.encode('utf-8')])
            res = self.socket.recv_multipart()

            if res[0].decode('utf-8') == 'in_interval':
                self.succesor = res[1].decode('utf-8')
                break
            else:
                self.succesor = res[1].decode('utf-8')


        #Hasta aqui nuevo
        ans = res[0].decode('utf-8')
        if ans == 'Y':
            self.succesor = res[1]
        while True:
            if ipGenesis2 == 0:
                resp = self.socket.recv_multipart()
                print(resp)
                idAnt = int(resp[0].decode('utf-8'))
                ipAnt = resp[1].decode('utf-8')
                print(ipAnt+'***')
                if (idAnt > self.intResp[0]) and (idAnt < self.intResp[1]):
                    self.socket.send_multipart([b'S', str(self.intResp[0]).encode('utf-8')])
                    self.intResp = [ idAnt+1, self.intResp[1]]
                    if 
                        self.pred = ipAnt
                    else: ##perdida
                        self.socket =self.pred == "a":self.context.socket(zmq.REQ)
                        self.connection = self.socket.connect("tcp://localhost:"+self.pred)
                        self.socket.send_multipart([self.ip.encode('utf-8'), ipAnt.encode('utf-8'), 'pred'.encode('utf-8')])
                        self.socket.recv_multipart()
                        self.socket = self.context.socket(zmq.REP)
                        self.connection = self.socket.bind("tcp://*:"+self.ip)
                        ipGenesis2 = 0
                

                elif resp[2].decode('utf-8') == "pred":
                    if resp[0].decode('utf-8') == self.pred:
                        self.pred = resp[1].decode('utf-8')
                        self.socket.send_multipart([b'ok'])
                    else:
                        self.socket.send_multipart([b'ok'])
                        self.socket =self.context.socket(zmq.REQ)
                        self.connection = self.socket.connect("tcp://localhost:"+self.pred)
                        self.socket.send_multipart([resp[0], ipAnt.encode('utf-8'), 'pred'.encode('utf-8')])
                        self.socket.recv_multipart()
                        self.socket = self.context.socket(zmq.REP)
                        self.connection = self.socket.bind("tcp://*:"+self.ip)
                        
                        ipGenesis2 = 0

                else: 
                    self.socket.send_multipart([b'N', self.pred.encode('utf-8')])
            
            else: 
                self.socket =self.context.socket(zmq.REQ)
                self.connection = self.socket.connect("tcp://localhost:"+ipGenesis2)
                hola = "hola"
                self.socket.send_multipart([str(self.id).encode('utf-8'), self.ip.encode('utf-8'), hola.encode('utf-8')])
                respPred = self.socket.recv_multipart()
                if respPred[0].decode('utf-8') == 'S':
                    self.pred = ipGenesis2##raro
                    self.intResp = [int(respPred[1].decode('utf-8')), int(self.id)]
                    self.socket = self.context.socket(zmq.REP)
                    self.connection = self.socket.bind("tcp://*:"+self.ip)
                    ipGenesis2 = 0

                else:
                    ipGenesis2 = respPred[1].decode('utf-8')
            print('*********')
            print('id:'+str(self.id))
            print('puerto:'+str(self.ip))
            print('predecesor:'+str(self.pred))
            print('intervalo:'+str(self.intResp))
            


def main():
    port_propio = sys.argv[1]
    port_connect = sys.argv[2]
    Id = sys.argv[3] #TODO este valor es temporal, luego se generara automaticamente
    initial = sys.argv[4] if sys.argv[4] else None

    server = Server(port_propio, port_connect)
    server.id = Id

    if initial:
        server.server_connection()
    else:
        server.server_connection(port_connect)
    


if __name__ == "__main__":
    main()
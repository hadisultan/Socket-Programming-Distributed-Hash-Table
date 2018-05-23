import socket
import thread
import sys
import hashlib
import math
import os.path
import uu
#001 means get me first suc
#002 means get me your pred
finglist = []
predlist = []
suclist = [] # (port)
filelist = []
ownport = int(sys.argv[1])
ip='localhost'
ownhash=0
nodesCAP = 10

#protocol addpred: "001 port ip ijbrnvcwd" -server process
def lookup_predindex(str):
	global predlist
	if len(predlist)==0:
		return -1

	port = int(str[4:8])
	ind = 0
	
	if predlist[0]>ownport: #cater overflow
		return 0
	
	for x in predlist:
		if port>x:
			break
		ind+=1
	return ind



def addpred(strng, client):
	global finglist
	global predlist
	index = lookup_predindex(strng)
	if index==-1: # no nodes
		predlist.insert(0,int(strng[4:8]))
		suclist.insert(0,int(strng[4:8]))
		mess = "101 "+str(ownport) +" * "+str(ownport)
		client.send(mess)
		client.close()
		makefingertable()
		return
	else:
		if index == 0:
			
			mess = "101 "+str(ownport) 
			for x in suclist:
				mess= mess+" "+str(x)
			mess=mess+" * "
			for x in predlist:
				mess=  mess+" "+str(x)
			predlist.insert(index,int(strng[4:8]))
			client.send(mess)
			client.close()
			return
		else:
			finde = int(math.log((10000-ownport) +(int(strng[4:8]) -1000),2)) # index in fingtable lowerbound
			mess=''
			if finglist[finde]==ownport:
				mess="001 "+str(predlist[0])
			else:	
				mess="001 "+str(finglist[finde])
			client.send(mess)
			client.close()
			return


def lookup_succindex(str):
	global suclist
	if len(suclist)==0:
		return -1

	port = int(str[4:8])
	ind = 0
	
	if suclist[0]<ownport: #cater overflow
		return 0
	
	for x in suclist:
		if port<x:
			break
		ind+=1
	return ind
			

#protocol: "xyz fvkmgnbijbrnvcwd" -server process
def addsuc(strng,client):
	global finglist
	global suclist
	index = lookup_succindex(strng) 
	if index==-1: # no nodes
		predlist.insert(0,int(strng[4:8]))
		suclist.insert(0,int(strng[4:8]))
		mess = "101 "+str(ownport) +" * "+str(ownport)
		client.send(mess)
		client.close()
		makefingertable()
		return
	else:
		if index == 0:
			
			mess = "101 "
			for x in suclist:
				mess= mess+" "+str(x)
			mess=mess+" * "+str(ownport) 
			for x in predlist:
				mess=  mess+" "+str(x)+" "
			suclist.insert(index,int(strng[4:8]))
			client.send(mess)
			client.close()
			return
		else:
			finde = int(math.log((int(strng[4:8]) - ownport),2)) # index in fingtable
			mess=''
			if finglist[finde]==ownport:
				mess="001 "+str(suclist[0])
			else:	
				mess="001 "+str(finglist[finde])
			
			client.send(mess)
			client.close()
			return


#following is the server side
def check(mess, client):
	global finglist
	global suclist
	global predlist
	global filelist
	if(len(mess)<3):
		client.send("4041")
		client.close()
		return
	if(mess[0:3] == "001"): #means get port
		if len(mess)>=8:
			if(int(mess[4:8])>ownport):
				addsuc(mess,client)
			else:
				addpred(mess, client)
			return
		else:
			client.send("4042")
			client.close()
			return
	if(mess[0:3] == "101"):
			return
	if(mess[0]) == '5':
		client.close()
		check=0
		for x in predlist:
			if x == int(mess[2:]):
				check=1
		if check == 1:
			predlist.remove(int(mess[2:]))
		check=0
		for x in suclist:
			if x == int(mess[2:]):
				check=1
		if check==1:
			suclist.remove(int(mess[2:]))
		return
	if(mess[0]) == '2':#set suc
		if suclist[0]!=int(mess[2:]):
			suclist.insert(0,int(mess[2:]))
		client.close()
	if(mess[0]) == '3':#set pred
		if predlist[0]!=int(mess[2:]):
			predlist.insert(0,int(mess[2:]))
		client.close()
	if (mess[0])=='9':
		client.send(str(suclist[0]))
		client.close()#gives suc
	if (mess[0])=='8':
		client.send(str(predlist[0]))
		client.close()#gives pred
	if (mess[0])=='7': #file receiving # 7 1234 filename --- where 1234 is the filehash
		for x in filelist:#if I already have the file
			if x==int(mess[2:6]):
				client.close()
				return
		if int(mess[2:6])>ownport:
			fingertableindex = int(math.log((int(mess[2:6]) - ownport),2))
			if ownport == finglist[fingertableindex]:
				filelist.append(int(mess[2:6]))
				client.close()
				filenaam = mess[12:]
				get(filenaam , int(mess[7:11])) #get copy
				if suclist[0]==int(mess[7:11]):
					return
				try: #send to suc for backup
					tem= socket.socket()
					tem.connect(('localhost',suclist[0]))
					mess = 'A'+mess[1:]
					strng = mess #cater file
					tem.send(strng)
					tem.close()
				except socket.error as err:
					print 'err'
				
				return
			elif finglist[fingertableindex]!=int(mess[7:11]):
				client.close()
				try: #send to suc for backup
					tem= socket.socket()
					tem.connect(('localhost',finglist[fingertableindex]))
					strng = mess #cater file
					tem.send(strng)
					tem.close()
				except socket.error as err:
					print 'err'
				return
			else:
				filelist.append(int(mess[2:6]))
				client.close()
				filenaam = mess[12:] 
				get(filenaam , int(mess[7:11])) #get copy
				try: #send to suc for backup
					tem= socket.socket()
					tem.connect(('localhost',suclist[0]))
					mess = 'A'+mess[1:]
					strng = mess #cater file
					tem.send(strng)
					tem.close()
				except socket.error as err:
					print 'err'
				return
		else:
			fingertableindex = int(math.log(((10000-ownport) +(int(mess[2:6]) -1000)),2))
			if ownport == finglist[fingertableindex]:
				filelist.append(int(mess[2:6]))
				client.close()
				filenaam = mess[12:] 
				get(filenaam , int(mess[7:11])) #get copy
				return
			elif finglist[fingertableindex]!=int(mess[7:11]):
				client.close()
				try: #send to suc for backup
					tem= socket.socket()
					tem.connect(('localhost',finglist[fingertableindex]))
					strng = mess #cater file
					tem.send(strng)
					tem.close()
				except socket.error as err:
					print 'err'
				return
			else:
				filelist.append(int(mess[2:6]))
				client.close()
				filenaam = mess[12:]
				get(filenaam , int(mess[7:11])) #get copy
				return

	if (mess[0])=='A': #file receiving # 7 1234 filename --- where 1234 is the filehash
			for x in filelist:#if I already have the file
				if x==int(mess[2:6]):
					client.close()
					return
				filelist.append(int(mess[2:6]))
				client.close()
				filenaam = mess[12:]
				get(filenaam , int(mess[7:11])) #get copy
				return
			
	if (mess[0:2])=="0 ": #requested file 
		filenaam = mess[2:]
		check = os.path.isfile(str(filenaam))
		if check == False:
			client.send("invalid file name"+str(filenaam))
			client.close()
			return
		sendfile(filenaam,client)
		client.close()
		return

	if (mess[0])=="6":#check file"6 " +str(ownport)+" "+str(filename) #cater file
		filenaam= mess[7:]
		sha1 = hashlib.sha1()
		sha1.update(filenaam)
		hashfilename = (int(sha1.hexdigest(), 16) % 9000)+1000
		for x in filelist:#if I already have the file
			if x==hashfilename:
				sendfile(filenaam,client)
				client.close()
				return
		#doesn't exist
		fingertableindex = -1
		if hashfilename>ownport:
			fingertableindex = int(math.log((hashfilename - ownport),2))
		else:
			fingertableindex = int(math.log(((10000-ownport) +(hashfilename) -1000),2)) 
		if finglist[fingertableindex]==ownport:
			client.send("4")
			client.close()
			return
		if finglist[fingertableindex]==int(mess[2:6]):
			client.send("4")
			client.close()
			return
		ms = "1 "+str(finglist[fingertableindex])
		client.send(ms)
		client.close()
		return
			
			


	return
		
		

	#insert additional req



#following is the server side
def func2(client, addr):
	#print "connected to:  ", addr
	while True:
		try:
			
			string = client.recv(1024)
			if len(string) != 0:
				check(string, client)	
		except socket.error as msg:
			#print "disconnected!!!"
			#makefingertable()
			break

		#print addr," said => ", string
def leaveall():
	for x in suclist:
		try:
			tem= socket.socket()
			tem.connect(('localhost',x))
			strng = "5 " +str(ownport)
			tem.send(strng)
			tem.close()
		except socket.error as err:
			print 'err'
	for x in predlist:
		try:
			tem= socket.socket()
			tem.connect((ip, x))
			strng = "5 " +str(ownport)
			tem.send(strng)
			tem.close()
		except socket.error as err:
			print err

def insert_list(msg):
	wrds = msg.split()
	check =0
	for x in wrds:
		if x == "*":
			check=1
		if len(x)<4:
			continue
		if check == 0:
			suclist.append(int(x))
		if check ==1:
			predlist.append(int(x))
	try:
		tem= socket.socket()
		tem.connect(('localhost',suclist[0]))
		strng = "3 " +str(ownport) #make me pred[0]
		tem.send(strng)
		tem.close()
	except socket.error as err:
		print 'err'
	try:
		tem= socket.socket()
		tem.connect((ip, predlist[0]))
		strng = "2 " +str(ownport) #make me suc[0]
		tem.send(strng)
		tem.close()
	except socket.error as err:
		print err

def getsuc():
	try:
		tem= socket.socket()
		tem.connect(('localhost',suclist[0]))
		strng = "9 " +str(ownport)
		tem.send(strng)
		mess = tem.recv(1024)
		if len(mess)!=0:
			suclist.append(int(mess))
		tem.close()
		return 1
	except socket.error as err:
		print 'err'
		return 0
	
def getpred():
	try:
		tem= socket.socket()
		tem.connect(('localhost',predlist[0]))
		strng = "8 " +str(ownport)
		tem.send(strng)
		mess = tem.recv(1024)
		if len(mess)!=0:
			predlist.append(int(mess))
		tem.close()
		return 1
	except socket.error as err:
		print 'err'
		return 0	

def refreshlist():
	global suclist
	global predlist
	failedcon = []
	if len(suclist)==0 and len(predlist)==0:
		return

	if len(suclist)<2:
		if len(suclist)==0:
			pass
		elif suclist[0]!=predlist[0]:
			check =getsuc()
			if check==0:
				del suclist[0]
				refreshlist()
				return
			else:
				refreshlist()
				return
	if len(predlist)<2:
		if len(predlist)==0:
			pass
		elif predlist[0]!=suclist[0]:
			check =getpred()
			if check==0:
				del predlist[0]
				refreshlist()
				return
			else:
				refreshlist()
				return
	
	if suclist[0]==predlist[0]:
		try:
			tem= socket.socket()
			tem.connect(('localhost',suclist[0]))
			strng = "9 " +str(ownport)
			tem.send(strng)
			mess = tem.recv(1024)
			tem.close()
			return
		except socket.error as err:
			print 'err'
			del suclist[0]
			del predlist[0]
			return

	for x in suclist:
		try:
			tem= socket.socket()
			tem.connect(('localhost',x))
			strng = "9 " +str(ownport)
			tem.send(strng)
			mess = tem.recv(1024)
			tem.close()
		except socket.error as err:
			print 'err'
			failedcon.append(x)

	for x in predlist:
		try:
			tem= socket.socket()
			tem.connect((ip, x))
			strng = "8 " +str(ownport)
			tem.send(strng)
			mess = tem.recv(1024)
			tem.close()
		except socket.error as err:
			print err
			failedcon.append(x)
	for x in failedcon:
		suclist = [z for z in suclist if z != x]
		predlist = [z for z in predlist if z != x]
	if len(failedcon)!=0:
		refreshlist()
	return
		
#following is the server side	
def func(port, ip):
	ownport = port
	s=  socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((ip,port))
	s.listen(10) # max number of connections
	while True: 
		client, addr = s.accept()
		thread.start_new_thread(func2, (client,addr))

def makefingertable():
	global finglist # store i+2^0 i+2^1 i+2^2 i+2^3
	global suclist
	if len(suclist)==0 and len(predlist)==0:
		finglist = []
		return
	refreshlist()
	finglist = []
	allnodes = []
	z =suclist[0]
	allnodes.append(z)
	while True:
		if z== ownport:
			break
		try:
			tem= socket.socket()
			tem.connect(('localhost',z))
			strng = "9 " +str(ownport)
			tem.send(strng)
			mess = tem.recv(1024)
			if len(mess)!=0:
				allnodes.append(int(mess))
			tem.close()
		except socket.error as err:
			print 'err'
			return
		z= int(mess)

	#print 'allnodes:',allnodes

	overf = 0
	i = 0
	temp = ownport
 #making 20^0 1 2 3 4 and so on
	while True:
		temp = ownport + pow(2,i)
		if pow(2,i)>=9000:
			break
		if temp>=10000:
			overf =1
			temp = (temp-10000)+1000
		if overf==1 and temp>ownport:
			break
		#print temp , " ", i , " ", pow(2,i)
		i += 1
		fin = allnodes[len(allnodes)-2]
		for x in allnodes:
		 	if fin>temp:
		 		if fin<x:
		 			continue
		 		elif x>temp:
			 			fin=x
			elif x>temp:
				fin =x
			elif x< fin:
				fin = x
		finglist.append(fin)
	print finglist
	return
			
def fileinsert():
	#insert fiel here
	global finglist
	global suclist
	global predlist
	global filelist
	print "Please write the name of the file to put:"
	filename = raw_input()
	check = os.path.isfile(str(filename))
	if check == False:
		print "invalid file... returning"
		return
	sha1 = hashlib.sha1()
	sha1.update(str(filename))
	makefingertable()
	hashfilename = (int(sha1.hexdigest(), 16) % 9000)+1000 #gives me hash of the filename
	fingertableindex = -1
	if len(suclist)==0 and len(predlist)==0:
		filelist.append(hashfilename)
		print "no other node so appended to self"
		return

	if hashfilename>ownport:
		fingertableindex = int(math.log((hashfilename - ownport),2))
	else:
		fingertableindex = int(math.log(((10000-ownport) +(hashfilename) -1000),2)) 
	#establish connection with that
	if finglist[fingertableindex]==ownport:
		filelist.append(hashfilename)
		try: #send to suc for backup
			tem= socket.socket()
			tem.connect(('localhost',suclist[0]))
			strng = "A " +str(hashfilename) +" "+str(ownport)+" "+str(filename) #cater file
			tem.send(strng)
			tem.close()
		except socket.error as err:
			print 'err'
		print "saved file to itself and sent to suc for backup"
		return
	#some other node
	filelist.append(hashfilename)#save it for the get of other nodes
	try: #send to suc for backup
		tem= socket.socket()
		tem.connect(('localhost',finglist[fingertableindex]))
		strng = "7 " +str(hashfilename) +" "+str(ownport)+" "+str(filename) #cater file
		tem.send(strng)
		tem.close()
	except socket.error as err:
		print 'err'
	return

def get (filename,port):
	#cater getting
	try: #send to suc for backup
		tem= socket.socket()
		tem.connect(('localhost',port))
		strng = "0 "+filename #cater file
		tem.send(strng)
		mess = tem.recv(1024)
		if mess == "OK":
			recvfile(filename,tem)
		else:
			print mess
		tem.close()
	except socket.error as err:
		print 'err'

def sendfile(filename,s):
	s.send("OK")
	f = open(filename,'rb')
	print
	l = f.read(1024)
	while (l):
		s.send(l)
		l = f.read(1024)
	f.close()
	s.shutdown(socket.SHUT_WR)
	print "sent"
	return
	
def recvfile(filename,c):
	check = os.path.isfile(str(filename))
	if check==True:
		print "file already exists so dumped"
		l= c.recv(1024)
		while (l):
			l = c.recv(1024)
		return
	f = open(filename,'wb')
	l = c.recv(1024)
	while (l):
		f.write(l)
		l = c.recv(1024)
	print "received"
	f.close()
	return

def getfile():
	print "Enter filename to download:"
	filename = raw_input()
	check = os.path.isfile(str(filename))
	if check == True:
		print "file already exists"
		return
	sha1 = hashlib.sha1()
	sha1.update(str(filename))
	makefingertable()
	hashfilename = (int(sha1.hexdigest(), 16) % 9000)+1000 #gives me hash of the filename
	fingertableindex = -1
	if len(suclist)==0 and len(predlist)==0:
		print "no such file exists"
		return

	if hashfilename>ownport:
		fingertableindex = int(math.log((hashfilename - ownport),2))
	else:
		fingertableindex = int(math.log(((10000-ownport) +(hashfilename) -1000),2)) 
	#establish connection with that
	if finglist[fingertableindex]==ownport:
		print "file not found"
		return
	#some other node
	stemp = finglist[fingertableindex]
	while True:
		try: #send to suc for backup
			tem= socket.socket()
			tem.connect(('localhost',stemp))
			strng = "6 " +str(ownport)+" "+str(filename) #cater file
			tem.send(strng)
			mess = tem.recv(1024)
			if mess == "OK":
				print "file found and receeving noww"
				recvfile(str(filename),tem)
				print "file downloaded"
				tem.close()
				return
			elif mess[0] == "1":
				if int(mess[2:])==ownport:
					tem.close()
					print "file not found"
					return
				elif int(mess[2:])==finglist[fingertableindex]:
					tem.close()
					print "file not found"
					return
				else:
					stemp = int(mess[2:])
			elif mess[0] == "4":
				print "file not found"
				tem.close()
				return
			tem.close()
		except socket.error as err:
			print 'err'
	return

def Main(): 
	print "Enter IP address: "
	ip = raw_input()
	print "Enter Port: "
	port = raw_input()
	port = int(port)
	#python dht.py 1234
	host = '127.0.0.1'
	ip= host
	thread.start_new_thread(func,(ownport, ip)) # starts a thread to listen
	newownport = hashlib.sha1()
	newownport.update(str(port))
	counter=0;
	if int(port) == -1:
		counter=3
	while True:	
		if counter==3:
			break
		if(port>0):
			print "cnnn"
			s = socket.socket() 
			s.connect((host, port))
			print"connected!!!!"
			strng = "001 " +str(ownport)
			print strng
			s.send(strng)
			while True:
				try:			
					string = s.recv(1024) #I will either receive a new port to connect or will remain connected to this
					
					#I will hope to get a final port here
					counter=0
					if string[0:3] =="101":
						insert_list(string)
						counter=3
						break
					elif string[0:3] =="001":
						print "recon"
						port=int(string[4:8])
						counter=0
						s.close()
						break
					elif string[0:3] == "404":
						print string
						counter+=1
						s.close()
						break
					else:
						print "hmmmm:" ,string
						s.close()
						break


				except socket.error as msg:
					counter+=1						
					print "disconnected!!!"
					break
		else:
			print "incorrect port input..."
			break
	print "suclist: ", suclist
	print "predlist: ", predlist
	makefingertable()
	# print sys.argv[1]  
	while True:
		print "Press 1 for successor list and predlist"
		print "press 2 to leave"
		print "press 3 to print pred and suclist"
		print "press 4 to print fingertable"
		print "press 5 to put file"
		print "press 6 to get file"
		inp = int(raw_input())
		if inp==2:
			leaveall()
			sys.exit()

		if inp==1:
			print "suclist: ", suclist
			print "predlist: ", predlist
			continue
		if inp==3:
			refreshlist()
			print "suclist: ", suclist
			print "predlist: ", predlist
			continue
		if inp==4:
			makefingertable()
		if inp==5:
			fileinsert()
		if inp==6:
			getfile()

	#start features here <-------------------------------------------------------INCOMPLETE CLIENT SIDE 
	print "suclist: ", suclist
	print "predlist: ", predlist 

if __name__=="__main__":
	Main()

'''
ENTRY SYSTEM
-new node enters
-connects to the node to get a node value
-the server node will compare values and either give it a new port value or make itself a successor
-the server node will be responsible to give port number to any node connected to it

Hashing
-Will be done by servers of each nodes
-along with storing pred and succ each server will store a lookup table
-the succ table will be filled with the hashe table 



'''
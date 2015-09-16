#!/usr/bin/python 
# -*- coding: utf-8 -*-


'''
 Pegar a saida o dump-acct e preparar para carga no SBR-L 
'''
import xmlrpclib
global dbhost
import time,sys
start_time = time.time()
#global server
import socket
ghost=socket.gethostname()
# onde esta o psacct
#print sys.argv
if len(sys.argv) < 5 : 
	print  """
       formato correto:
	   /home/coletor/bin/coletor.py /var/log/account/pacct.0  /home/coletor/bin/coletor.conf nodo01 localhost 56667
          """
	sys.exit()


file_name = sys.argv[1]
file_conf = sys.argv[2]
# define o host
try:
  HOST=sys.argv[3]
except:
   HOST=ghost

if ghost ==  "nodo01":
   dbhost  = "localhost"
#'http://localhost:56667'
rpc_server = sys.argv[4]
rpc_port   = sys.argv[5]

   
   
import ConfigParser
Config = ConfigParser.ConfigParser()
Config.read(file_conf)

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


global  dump_acct
'''
dbhost = ConfigSectionMap("mysql")['dbhost']
dbport = int(ConfigSectionMap("mysql")['dbport'])
dbuser = ConfigSectionMap("mysql")['dbuser']
dbpass = ConfigSectionMap("mysql")['dbpass']
dbbase = ConfigSectionMap("mysql")['dbbase']
'''
dump_acct = ConfigSectionMap("dump")['acct']


### Cabecalhos ###
import syslog
import sys
### especial logos no syslog.
def log_msg(msg,f):
  name="Coletor_acct"
  # EMERG=0   Alert=1 CRIT=2 ERR=3 WARN=4 NOTICE =5  INFO=6 DEBUG=7  
  prior =  [syslog.LOG_EMERG,syslog.LOG_ALERT,syslog.LOG_CRIT,syslog.LOG_ERR,syslog.LOG_WARNING,syslog.LOG_NOTICE,syslog.LOG_INFO,syslog.LOG_DEBUG]  
  #print prior[f],  name
  syslog.openlog(name,prior[f])
  syslog.syslog(prior[f],msg)
  syslog.closelog()

from datetime import datetime
import sqlite3 as lite
import shlex, subprocess


## usa base em memorias, ganho tempo e nao deixo lixo
con = lite.connect(':memory:')
msg_coletor = []

####  INICIO FUNCAO DE TRANSFERENCIAS ###


def valid_host(HOST):
	ServidorRpc="http://%s:%s" % (rpc_server, rpc_port)
	proxy = xmlrpclib.ServerProxy(ServidorRpc, verbose=False)
	return proxy.valida_host(HOST)

def remote_into(Linhas,N,host):
	ServidorRpc="http://%s:%s" % (rpc_server, rpc_port)
	proxy = xmlrpclib.ServerProxy(ServidorRpc, verbose=False)
	return proxy.remote_into(Linhas,N,host)

def remote_local_users(Linhas,ghost):
	ServidorRpc="http://%s:%s" % (rpc_server, rpc_port)
	proxy = xmlrpclib.ServerProxy(ServidorRpc, verbose=False)
	return proxy.remote_local_users(Linhas,ghost)

### FIM DE TRANSFERENCIA	
	

def local_user():
 #getent  gshadow  ## quem tem shadow eh local!
 #  getent passwd $USER ;a
 # pega username
 command_line = "sudo /usr/bin/getent shadow" 
 args = shlex.split(command_line)
 p = subprocess.Popen(args, stdout=subprocess.PIPE)
 result= p.communicate()[0]
 Inserts=[]
 for line in result.split("\n",):
    sline=line.rstrip()
    sline=sline.split(':')[0]
    if not line == '':
      Inserts.append(sline)
 #pega uid
 Linhas=[]
 for username in Inserts:
   command_line = "/usr/bin/id -u %s " % (username)
   args = shlex.split(command_line)
   p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   result_uid= p.communicate()[0]

   command_line = "/usr/bin/id -g %s " % (username)
   args = shlex.split(command_line)
   p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   result_gid= p.communicate()[0]

   if not result_uid == "" :
             if not result_uid == "" : 
               uid=result_uid.split("\n",)[0]
               gid=result_gid.split("\n",)[0]
               balde= "%s,%s,%s,%s" % (uid, gid , username,"grupo")
               Linhas.append(balde)

 return Linhas

############  FUNCOES SQLITE 

# prepara dados para a base em memoria
def insql(line):
  content = []
  b=line.split("|")
  if len(b) == 11 : 
   # 0                1      2          3          4       5      6       7         ?8         9            10
   # tput            |v3|     7.00|     0.00|     7.00|     0|     0|  1824.00|     0.00|    2825     2816|Wed Jul  1 15:47:56 2015
   command=b[0].strip()
   time_user=b[2].strip()
   time_system=b[3].strip()
   time_effective=b[4].strip()
   user_id=b[5].strip()
   group_id=b[6].strip()
   memory=b[7].strip()
   io=b[9].strip()
   time_date=b[10].strip()

  else:
   command=b[0].strip()
   time_user=b[1].strip()
   time_system=b[2].strip()
   time_effective=b[3].strip()
   user_id=b[4].strip()
   group_id=b[5].strip()
   memory=b[6].strip()
   if len(b) == 8 :
      io="zerado"
      time_date=b[7].strip()
   else:
     io=b[7].strip()
     time_date=b[8].strip()


  date_object = datetime.strptime(time_date, '%a %b %d %H:%M:%S %Y')
  #full_row='(NULL,"%s",%s,%s,%s,%s,%s,%s,"%s","%s")'  % (command,time_user,time_system,time_effective,user_id,group_id,memory,io,date_object)
  full_row='INSERT INTO acct VALUES("%s",%s,%s,%s,%s,%s,%s,"%s","%s")'  % (command,time_user,time_system,time_effective,user_id,group_id,memory,io,date_object)
  row="%s" %full_row
  return row


def intodb(Inserts):
   try:
     result=0	  
     with con:
       cur = con.cursor()
       cur.execute("DROP TABLE IF EXISTS acct")
       cur.execute("CREATE TABLE acct ( comando  TEXT , t_user REAL , t_system  REAL, t_effective REAL , uid INT, gid INT , memoria REAL , io TEXT, d_time TEXT)")
       for tulpas in Inserts:
	     cur.execute(tulpas)
   except:
      result=1
   return result

def querys(n,host):
  #con = lite.connect('test.db')
  #Query=["SELECT  uid,sum(t_effective),sum(t_user),sum(t_system),date(d_time)  FROM acct  GROUP BY  uid,date(d_time) ORDER BY  uid ",
  #   "SELECT comando,uid,sum(t_effective),sum(t_user),sum(t_system),date(d_time)  FROM  acct GROUP BY  comando,uid,date(d_time) ORDER BY  comando " ]

  Query=["SELECT  uid,sum(t_effective),sum(t_user),sum(t_system),date(d_time)  FROM acct  GROUP BY  uid,date(d_time)",
     "SELECT comando,uid,sum(t_effective),sum(t_user),sum(t_system),date(d_time)  FROM  acct GROUP BY  comando,uid,date(d_time)" ]

  with con:
    cur = con.cursor()
    cur.execute(Query[n])
    rows = cur.fetchall()
    linhas=[]
    for row in rows:
        linhas.append(row)

  return linhas
################# FIM FUNCOES  SQLITE


# verifica se o host tem permissao para executar

result=valid_host(HOST)
if int(result) == 1:
  msg=HOST+" Falha, sem permissao para coleta"
  f=3
  log_msg(msg,f)
  print msg
  sys.exit()

# dumpacct para a memoria
command_line = "%s %s" % (dump_acct,file_name)

args = shlex.split(command_line)
p = subprocess.Popen(args, stdout=subprocess.PIPE)
result= p.communicate()[0]
Inserts=[]

for line in result.split("\n",):
   Dline=line.rstrip()
   if not line == '':
     Inserts.append(insql(Dline))

	 
	 
	 
N_insert= str(len(Inserts))
msg_user = "PACCT " + N_insert
msg_coletor.append(msg_user)

# registro na base interna
b=intodb(Inserts)   
if b == 1 :
  msg=HOST+" Falha, no sqlite"
  print msg
  f=3
  log_msg(msg,f)
  sys.exit()


# insere usuarios na base 
Efetivo_uid=querys(0,HOST)
N_user_cons=str(len(Efetivo_uid))
N=0
N_regU=str(remote_into(Efetivo_uid,N,HOST))
msg_user = "Consolidados: Users %s/%s " %(N_user_cons,N_regU)
msg_coletor.append(msg_user)

# insere comandos na base
Efetivo_comando=querys(1,HOST)
N_cmd_cons=str(len(Efetivo_comando))
N=1
N_regCMD=str(remote_into(Efetivo_comando,N,HOST))
msg_user = "Comandos %s/%s " %(N_cmd_cons,N_regCMD)
msg_coletor.append(msg_user)

# insere users local na base de dados.
Balde=local_user()
registro = str(remote_local_users(Balde,HOST) )
N_Balde=str(len(Balde))
msg_user = "Users_Locais %s/%s " %(N_Balde,registro)
msg_coletor.append(msg_user)


tempo_execution= (time.time() - start_time)

#print "|".join(msg_coletor)
msg="|".join(msg_coletor)
tempo_execution= (time.time() - start_time)
msg_user = "Tempo Exec:%.2fs" %(tempo_execution)
msg_coletor.append(msg_user)

msg="|".join(msg_coletor)

f=5
log_msg(msg,f)
print msg
sys.exit()

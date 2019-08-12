# -*- coding: UTF-8 -*-
import datetime
import logging
import os
import telnetlib
import time
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock


class ZooKeeperLock:
    def __init__(self, hosts, lock_path, timeout=1, logger=None):
        self.hosts = hosts
        self.zk_client = None
        self.timeout = timeout
        self.logger = logger
        self.lock_handle = None
        self.lock_path = lock_path

        self.create_lock()

    def create_lock(self):
        try:
            self.zk_client = KazooClient(hosts=self.hosts, logger=self.logger, timeout=self.timeout)
            self.zk_client.start(timeout=self.timeout)
        except Exception, ex:
            self.init_ret = False
            self.err_str = "Create KazooClient failed! Exception: %s" % str(ex)
            logging.error(self.err_str)
            return

        try:
            self.lock_handle = Lock(self.zk_client, self.lock_path)
        except Exception, ex:
            self.init_ret = False
            self.err_str = "Create lock failed! Exception: %s" % str(ex)
            logging.error(self.err_str)
            return

    def destroy_lock(self):
        if self.zk_client != None:
            self.zk_client.stop()
            self.zk_client = None

    def acquire(self, blocking=True, timeout=None):
        if self.lock_handle == None:
            return None

        try:
            return self.lock_handle.acquire(blocking=blocking, timeout=timeout)
        except Exception, ex:
            self.err_str = "Acquire lock failed! Exception: %s" % str(ex)
            logging.error(self.err_str)
            return None
        
    def release(self):
        if self.lock_handle == None:
            return None
        return self.lock_handle.release()

    def __del__(self):
        self.destroy_lock()


class Properties:

    def __init__(self, file_name):
        self.file_name = file_name
        self.properties = {}

        try:
            fopen = open(self.file_name, 'r')
            for line in fopen:
                line = line.strip()
                if line.find('=') > 0 and not line.startswith('#'):
                    strs = line.split('=')
                    self.properties[strs[0].strip()] = strs[1].strip()
        except Exception, e:
            raise e
        else:
            fopen.close()

    def get(self, key, default_value=''):
        if key in self.properties:
            return self.properties[key]
        return default_value


class Utils:
    def do_telnet(self, Host):
        try:
            tn = telnetlib.Telnet(Host, port=80, timeout=5)
            tn.close()
        except:
            return False
        return True

    def kill_port(self, port):
        try:
            rs = os.popen('lsof -i:{0}|grep java|grep TCP'.format(int(port))).read()
            pid = int(' '.join(rs.split()).split()[1])
            if( pid>0 ):
                os.popen('kill -9 {0}'.format(pid))
        except:
            return False
        return True

    def hdfs_nn_is_active(self,nn):
        return os.popen('hdfs haadmin -getServiceState {0}'.format(nn)).read().strip()


def restart_zkfc(props,utils,logging):
    stop_zfkc_cmd = props.get("stop_zfkc_cmd")
    start_zfkc_cmd = props.get("start_zfkc_cmd")
    zkfc_port = props.get("zkfc_port")

    # 停止zkfc
    logging.info(stop_zfkc_cmd)
    stop_zfkc_status = os.system(stop_zfkc_cmd)
    if stop_zfkc_status == 0:
        logging.info('stop zkfc :succ')
    else:
        logging.info('stop zkfc :faild')

    # 根据zkfc端口号kill掉对应的pid
    utils.kill_port(zkfc_port)

    # 启动zkfc
    start_zfkc_status = os.system(start_zfkc_cmd)
    if start_zfkc_status == 0:
        logging.info('{0},ok'.format(start_zfkc_cmd))
    else:
        logging.error('{0},ok'.format(start_zfkc_cmd))


def main():

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s -%(module)s:%(filename)s-L%(lineno)d-%(levelname)s: %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    props = Properties("config.properties")
    utils = Utils()

    nn_has_standby_num = None

    zookeeper_hosts = props.get("zookeeper_hosts")
    lock_name = props.get("zk_lock_name")
    nn_list = props.get("nn_list").split(',')



    while True:

        lock = ZooKeeperLock(zookeeper_hosts, lock_name, logger=logger)
        #申请锁
        ret = lock.acquire()

        #拿到锁
        if ret:
            try:
                logging.info("get zk lock! Ret: {0}".format(ret))

                # 获取nn状态为standby的数量
                nn_has_standby_num = 0
                for nn in nn_list:
                    status = utils.hdfs_nn_is_active(nn)
                    if (status == 'standby'):
                        nn_has_standby_num = nn_has_standby_num + 1
                        logging.error('hdfs haadmin -getServiceState {0} : {1}'.format(nn, status))
                    elif (status == 'active'):
                        logging.info('hdfs haadmin -getServiceState {0} : {1}'.format(nn, status))
                    else:
                        logging.error('hdfs haadmin -getServiceState {0} : {1}'.format(nn, 'Null'))

                # nn都为standby时重启zfkc
                if (nn_has_standby_num == len(nn_list)):
                    logging.info('restart zkfc')
                    restart_zkfc(props,utils,logging)
                elif (nn_has_standby_num > 0):
                    logging.info('nn check status ok')
                else:
                    logging.info('nn check status Null')

            finally:
                logging.info("release zk lock")
                lock.release()

        # 拿不到锁时
        else:
            logging.error("can't get lock! Ret: {0}".format(ret))


        #sleep 10秒
        logging.info("===========================================================")
        time.sleep(10)








if __name__ == "__main__":
    try:
        main()
    except Exception, ex:
        print "Ocurred Exception: %s" % str(ex)
        quit()

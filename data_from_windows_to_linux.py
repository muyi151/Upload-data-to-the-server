import os
import paramiko
import re
from collections import defaultdict

# 初始化SSH客户端
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# 连接到目标服务器
try:
    print("连接到服务器...")
    ssh.connect(hostname='111.111.11.11', username='aaaaa', password='bbbbbbbbb')
    print("连接成功")
except Exception as e:
    print(f"连接失败，错误信息：{e}")

# 创建SFTP客户端
sftp = ssh.open_sftp()

# 指定本地文件夹路径
local_dir = "D:\\FY4data"

# 文件计数器
file_count = 0

# 创建一个字典来存储每个远程目录的文件计数
dir_file_count = defaultdict(int)

# 遍历本地文件夹及其子文件夹中的文件
for root, dirs, files in os.walk(local_dir):
    for file_name in files:
        if file_name.lower().endswith('.nc'):
        #if file_name.lower().endswith('.hdf'):
            # 正则表达式用于匹配年份、月份和 "CTT"
            match = re.search(r"L2-_(\w+)-_MULT_NOM_(\d{4})(\d{2})", file_name)
            #match = re.search(r"L1-_(\w+)-_MULT_NOM_(\d{4})(\d{2})", file_name)
            #print("匹配到的文件名:", file_name)
            if match:
                 # 提取词语（例如 "CTT-"），年份和月份
                word, year, month = match.groups()
                # print("提取到的词语:", word)
                # print("提取到的年份:", year)
                # print("提取到的月份:", month)
                
                # 构建远程目录路径
                remote_dir = f"/home/aaaaa/{year}/CTT/{int(month):02d}/"
                #print("远程目录路径:", remote_dir)
                
                # 检查远程目录是否存在，如果不存在则创建
                current_dir = ''
                for dir in remote_dir.split('/'):
                    if dir:
                        current_dir += '/' + dir  # 使用字符串连接来构建路径
                        try:
                            sftp.stat(current_dir)
                        except Exception as e:
                            print(f"目录 {current_dir} 不存在，尝试创建...")
                            try:
                                sftp.mkdir(current_dir)
                                print(f"成功创建目录：{current_dir}")
                            except Exception as e:
                                print(f"创建目录 {current_dir} 时发生错误：{e}")
                                continue

                # 拼接本地文件的完整路径
                local_file = os.path.join(root, file_name)
                
                # 检查本地文件是否存在
                if not os.path.exists(local_file):
                    print(f"本地文件 {local_file} 不存在")
                    continue
                
                # 拼接远程文件的完整路径
                remote_file = os.path.join(remote_dir, file_name)
                
                # 获取本地文件的大小
                local_file_size = os.path.getsize(local_file)
                
                try:
                    # 获取远程文件的大小
                    remote_file_stat = sftp.stat(remote_file)
                    remote_file_size = remote_file_stat.st_size
                    
                    # 如果本地文件大小小于或等于远程文件大小，则跳过
                    if local_file_size <= remote_file_size:
                        print(f"文件 {file_name} 已存在，且大小小于或等于远程文件的大小，跳过上传")
                        continue
                except IOError:
                    # 文件不存在，可以继续上传
                    pass
                
                # 上传文件
                try:
                    sftp.put(local_file, remote_file)
                    # 增加文件计数器
                    file_count += 1
                    dir_file_count[remote_dir] += 1
                    print(f"文件 {file_name} 上传成功到 {remote_dir}，这是第 {file_count} 个上传成功的文件")
                except PermissionError:
                    print(f"权限错误：无法上传文件到 {remote_dir}")
                except Exception as e:
                    print(f"发生错误：{e}")

# 关闭SFTP和SSH连接
sftp.close()
ssh.close()

# 输出每个远程目录的文件计数
for dir_path, count in dir_file_count.items():
    print(f"目录 {dir_path} 上传了 {count} 个文件")

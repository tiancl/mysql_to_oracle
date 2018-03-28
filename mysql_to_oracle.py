#coding:utf-8
import pymysql,re 
  


db= pymysql.connect(host="localhost",user="root",  
                            password="root",db="books",port=3306,charset="utf8") 

#table='books_info'
#oracle目的用户
target_user='test'
#去mysql上查看所有的表
table_sql='show tables'
cur=db.cursor() 
cur.execute(table_sql)
tables_data=cur.fetchall()
#tables_data=['china_bank_pay']
for table in tables_data:
    #获取表名
    table=list(table)[0]
    print('--%s表建表语句' % table)
    #查看表字段情况
    show_sql="desc %s" % table
    #查看表索引情况
    show_index_sql='show index from %s ' % table
    #查看所有字段的注释
    comment_sql='SHOW FULL COLUMNS FROM %s' % table
    #oracle和mysql字段对应关系
    dic={'bigint': 'NUMBER', 'bit': 'RAW', 'blob': 'BLOB', 'char': 'CHAR',
          'date': 'DATE', 'datetime': 'DATE', 'decimal': 'NUMBER', 'double': 'NUMBER',
           'doubleprecision': 'FLOAT', 'enum': 'VARCHAR2', 'float': 'FLOAT', 'int': 'NUMBER', 
           'integer': 'NUMBER', 'longblob': 'BLOB', 'longtext': 'CLOB', 'mediumblob': 'BLO',
            'mediumint': 'NUMBER', 'mediumtext': 'CLOB', 'numeric': 'NUMBER', 'real': 'FLOAT', 
            'set': 'VARCHAR2', 'smallint': 'NUMBER', 'text': 'CLOB', 'time': 'DATE', 
            'timestamp': 'DATE', 'tinyblob': 'RAW', 'tinyint': 'NUMBER', 'tinytext': 'VARCHAR2', 
            'varchar': 'VARCHAR2', 'year': 'NUMBER','binary':'VARCHAR2'}
    #oracle所有关键字，避免mysql表字段用到了这些关键字，报错
    keyword=['ACCESS','ADD','ALL','ALTER','AND','ANY','AS','ASC','AUDIT','BETWEEN','BY','CHAR','CHECK',
             'CLUSTER','COLUMN','COMMENT','COMPRESS','CONNECT','CREATE','CURRENT','DATE','DECIMAL','DEFAULT',
             'DELETE','DESC','DISTINCT','DROP','ELSE','EXCLUSIVE','EXISTS','FILE','FLOAT','FOR','FROM','GRANT',
             'GROUP','HAVING','IDENTIFIED','IMMEDIATE','IN','INCREMENT','INDEX','INITIAL','INSERT','INTEGER',
             'INTERSECT','INTO','IS','LEVEL','LIKE','LOCK','LONG','MAXEXTENTS','MINUS','MLSLABEL','MODE','MODIFY',
             'NOAUDIT','NOCOMPRESS','NOT','NOWAIT','NULL','NUMBER','OF','OFFLINE','ON','ONLINE','OPTION','OR','ORDER',
             'PCTFREE','PRIOR','PRIVILEGES','PUBLICRRAW','RENAME','RESOURCE','REVOKE','ROW','ROWID','ROWNUM','ROWS',
             'SELECT','SESSION','SET','SHARE','SIZE','SMALLINT','START','SUCCESSFUL','SYNONYM','SYSDATE','TABLE','THEN',
             'TO','TRIGGER','UID','UNION','UNIQUE','UPDATE','USER','VALIDATE','VALUES','VARCHAR','VARCHAR2','VIEW',
             'WHENEVER','WHERE','WITH']
    cur=db.cursor()  
    cur.execute(show_sql)
    data=cur.fetchall()
    data_len=len(data)
    cur.execute(show_index_sql)
    index_data=cur.fetchall()
    cur.execute(comment_sql)
    comment_datas=cur.fetchall()
    #func=lambda x,y:re.sub('(\d+)',re.match('(\d+)',x).group(1)*3,x) if y=='varchar' else x
    #func=lambda x,y:re.sub('(\d+)',re.match('(\d+)',x).group(1)*3,x) if y=='varchar' else x
    #传入替换后的oracle字符类型和mysql处理后的字符类型
    #mysql数值类型无校验，例如bigint最大为20，但是可以在建表写64，所以这里处理了超长的数值类型字段
    def func1(x,y):
        if y in ('bigint','int','tinyint','smallint','year','mediumint','integer','double','decimal','decimal') and int(re.findall('\((.*?)\)',x)[0].split(',')[0])>38:
            return re.sub('\((.*?)\)','('+str(38)+')',x).replace('unsigned','')
        else:
            return x.replace('unsigned','')
    #替换时间默认值，未完待续
    def huan(para):
        if para!=None:
            if para=='CURRENT_TIMESTAMP':
                return 'sysdate'
            else:
                return "'%s'" % para
        else:
            return ""
    #替换默认格式        
    def defaultvalue(para):
        return ' default %s' % huan(para) if para!=None and para!='' else ''
    #处理关键值格式     
    def ifkey(key):
        if key=='PRI':
            return ' primary key'
        else:
            return ''
    #处理空值格式
    def ifnull(isnull):
        if isnull=='NO':
            return ' not null'
        else:
            return ''
    #做关键字处理，如果是关键字，直接别名
    def keywordreduce(para):
        if para.upper() in keyword:
            return para+'1'
        else:
            return para
    #构建建表语句            
    result='create table  %s.%s ( ' % (target_user,table)
    #print(result)
    #索引列表
    index_name=[]
    #迭代建表语句
    for k,i in enumerate(data):
        if k+1==data_len:
            result=result+('%s' % keywordreduce(list(i)[0]))+' '+func1(list(i)[1].replace(list(i)[1].split('(')[0],dic['%s' % list(i)[1].split('(')[0]]),list(i)[1].split('(')[0])+ifkey(list(i)[3])+defaultvalue(list(i)[4])+ifnull(list(i)[2])+'\n'
            index_name.append(list(i)[0]) if list(i)[3]=='MUL' else ''      
                  
        else:
            result=result+('%s' % keywordreduce(list(i)[0]))+' '+func1(list(i)[1].replace(list(i)[1].split('(')[0],dic['%s' % list(i)[1].split('(')[0]]),list(i)[1].split('(')[0])+ifkey(list(i)[3])+defaultvalue(list(i)[4])+ifnull(list(i)[2])+' , '+'\n'
            index_name.append(list(i)[0]) if list(i)[3]=='MUL' else ''
    print(result+' );')
    #迭代语句注释
    for comment_data in comment_datas:
        print("comment on column  %s.%s.%s IS '%s';" % (target_user,table,keywordreduce(list(comment_data)[0]),list(comment_data)[8]))
    #迭代索引
    dic={}
    for de in index_data:
         dic.setdefault(list(de)[2],[]).append(list(de)[4])
    for k,v in enumerate(dic.items()):
        if v[0]!='PRIMARY':
            print('create index %s.idx_%s_%s  on %s (%s);' %(target_user,table[1:20],k,table,','.join(v[1])))
    print('--%s表建表语句结束' % table)
import pymysql
import json



sql = '''
CREATE TABLE review (id INT,name VARCHAR(200) NOT NULL,screen_name VARCHAR(300),location VARCHAR(300),url VARCHAR(100),description VARCHAR(500),protected VARCHAR(300),followers_count INT,friends_count INT,listed_count INT,created_at VARCHAR(100),favourites_count INT,verified VARCHAR(100),statuses_count INT,is_translator VARCHAR(300),profile_image_url_https VARCHAR(200),profile_banner_url VARCHAR(200), default_profile_image VARCHAR(100),translator_type VARCHAR(100),email VARCHAR(100),phone VARCHAR(100))'''
def daoru(db):
    cursor = db.cursor()
    cursor.execute("select version()")
    data = cursor.fetchone()
    print("database version: %s" %data)
    #cursor.execute("CHARACTER SET utf8mb4")
    cursor.execute(sql)
    
def reviewdata_insert(db):
    with open('E:\Desktop\落查\\twitter\\twitter.json',encoding='utf-8') as f:
        i = 0
        while True:
            i+=1
            print(u'正在载入第%s行......' %i)
            try:
                result = []
                lines = f.readline()
                review_text = json.loads(lines)
                #print(str(review_text))
    
                banner_url = list((dict(review_text)).keys())[16]
                phone = list((dict(review_text)).keys())[-1]
                #print(phone)
                #print(type(banner_url))
                if banner_url == 'profile_banner_url' and phone == 'phone':
                    result.append((review_text['id'],review_text['name'],review_text['screen_name'],review_text['location'],review_text['url'],review_text['description'],review_text['protected'],review_text['followers_count'],review_text['friends_count'],review_text['listed_count'],review_text['created_at'],review_text['favourites_count'],review_text['verified'],review_text['statuses_count'],review_text['is_translator'],review_text['profile_image_url_https'],review_text['profile_banner_url'],review_text['default_profile_image'],review_text['translator_type'],review_text['phone']))
                    print('banner和phone在')
                    inert_re = "insert into review(id,name,screen_name,location,url,description,protected,followers_count,friends_count,listed_count,created_at,favourites_count,verified,statuses_count,is_translator,profile_image_url_https,profile_banner_url,default_profile_image,translator_type,phone) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor = db.cursor()
                    cursor.executemany(inert_re,result)
                    db.commit()
                elif banner_url == 'profile_banner_url' and phone == 'email':
                    result.append((review_text['id'],review_text['name'],review_text['screen_name'],review_text['location'],review_text['url'],review_text['description'],review_text['protected'],review_text['followers_count'],review_text['friends_count'],review_text['listed_count'],review_text['created_at'],review_text['favourites_count'],review_text['verified'],review_text['statuses_count'],review_text['is_translator'],review_text['profile_image_url_https'],review_text['profile_banner_url'],review_text['default_profile_image'],review_text['translator_type'],review_text['email']))
                    print('banner和email在')
                    inert_re = "insert into review(id,name,screen_name,location,url,description,protected,followers_count,friends_count,listed_count,created_at,favourites_count,verified,statuses_count,is_translator,profile_image_url_https,profile_banner_url,default_profile_image,translator_type,email) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor = db.cursor()
                    cursor.executemany(inert_re,result)
                    db.commit()
                elif phone == 'email' and  banner_url != 'profile_banner_url':
                    result.append((review_text['id'],review_text['name'],review_text['screen_name'],review_text['location'],review_text['url'],review_text['description'],review_text['protected'],review_text['followers_count'],review_text['friends_count'],review_text['listed_count'],review_text['created_at'],review_text['favourites_count'],review_text['verified'],review_text['statuses_count'],review_text['is_translator'],review_text['profile_image_url_https'],review_text['default_profile_image'],review_text['translator_type'],review_text['email']))
                    print('email在')
                    inert_re = "insert into review(id,name,screen_name,location,url,description,protected,followers_count,friends_count,listed_count,created_at,favourites_count,verified,statuses_count,is_translator,profile_image_url_https,default_profile_image,translator_type,email) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor = db.cursor()
                    cursor.executemany(inert_re,result)
                    db.commit()
                
                elif phone == 'phone' and banner_url != 'profile_banner_url':
                    result.append((review_text['id'],review_text['name'],review_text['screen_name'],review_text['location'],review_text['url'],review_text['description'],review_text['protected'],review_text['followers_count'],review_text['friends_count'],review_text['listed_count'],review_text['created_at'],review_text['favourites_count'],review_text['verified'],review_text['statuses_count'],review_text['is_translator'],review_text['profile_image_url_https'],review_text['default_profile_image'],review_text['translator_type'],review_text['phone']))
                    print('phone在')
                    inert_re = "insert into review(id,name,screen_name,location,url,description,protected,followers_count,friends_count,listed_count,created_at,favourites_count,verified,statuses_count,is_translator,profile_image_url_https,default_profile_image,translator_type,phone) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor = db.cursor()
                    cursor.executemany(inert_re,result)
                    db.commit()
                print(result)
                
            
            except Exception as e:
                db.rollback()
                print(e)
                break
    f.close()
            
            
if __name__ == '__main__':
    db = pymysql.connect(host='192.168.237.130',port=20999,user='root',passwd='123qwe..',db='twitter',charset="utf8mb4")
    cursor = db.cursor()
    daoru(db)
    reviewdata_insert(db)
    cursor.close()
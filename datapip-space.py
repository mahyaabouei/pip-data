import pymongo 
import pandas as pd
from django.utils import timezone
import datetime
import requests
import json
import random

def login () :

    '''
    -To login all functions
    '''

    payload = json.dumps({
    "mobile": "09037976393",
    "encrypted_response": "gAAAAABmZqmOinqv2607-DW26KcYtH5O7axZ84LswXVsXM_dMhZHZeNrvOyJnQTkgRcq0mXCcpqt65q3xF1yT9Dbpd6XEER4Hw==",
    "captcha": ""
    })
    headers = {
    'Content-Type': 'application/json'
    }

    result = requests.post(url='http://127.0.0.1:8000/api/otp/' , headers=headers, data=payload )
    url = "http://127.0.0.1:8000/api/login/"

    payload = json.dumps({
    "mobile": "09037976393",
    "code": "11111"
    })
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = json.loads(response.content)
    return response['access']


def read():
    '''
    -Gets data from farasahm2 (registerNoBours)
    -Matching common fields :
        نام و نام خانوادگی => first_name
        کد ملی  => national_code
        شماره تماس => mobile
        صادره => issue
    -Change some fields :
        username =>کد ملی
        password =>شماره تماس
    -Delete another fields
    -And sets empty data to none
    '''

    token = login ()
    url = "http://127.0.0.1:8000/api/user/"
    headers = {
  'Content-Type': 'application/json' , 
  'Authorization' : 'Bearer ' + token
                }
    client = pymongo.MongoClient('mongodb://192.168.11.13:27017')
    
    try:
        db = client['farasahm2']
        collection = db['registerNoBours']
        data = collection.find({},{'_id':0})

        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset='کد ملی')
        df = df.drop(columns=[ 'تعداد سهام', 'symbol', 'rate' ,'تاریخ تولد','بانک','کدبورسی','شماره حساب','شماره شناسنامه'])
        df['username'] = df['کد ملی']
        df['create_at'] = str(datetime.datetime.now())
        df['expiration'] = str(datetime.datetime.now() + datetime.timedelta(days=365))
        df['date_last_act'] = str(datetime.datetime.now())
        df['status'] = True
        df['is_person'] = [len(str(x)) ==10 for x in df['username']]
        df['email'] = None
        df['last_name'] = None
        df['password'] = df['شماره تماس'].astype(str).apply(lambda x: x[-4:] if pd.notnull(x) else '')
        df['phone'] = None
        df['address'] = None
        df['profile_picture'] = None
        df['date_birth'] = None
        df['profile_picture'] = None
        df['gender'] = 'U'
        df['card_number_bank'] = None
        df['marital'] = None
        df['groups'] = [[] for x in df['username']]
        # df['user_permissions'] = [[] for x in df['username']]

        df = df.rename(columns={'نام و نام خانوادگی':'first_name','کد ملی':'national_code','شماره تماس':'mobile','صادره':'issue'})
        print(df)
        # time.sleep(1000)
        df = df.to_dict('records')
        for i in df :
            print('-'*25)
            print(i)
            payload = json.dumps(i)
            result = requests.post(url=url , headers= headers, data=payload)
            print(result.content)


        
        
    finally:
        client.close()




def folan () :

    '''
    -Set user as data frame 
    '''

    url = "http://127.0.0.1:8000/api/user/"

    users =pd.DataFrame(json.loads (requests.get(url=url).content))
    print(users)



def  create_company () :

    '''
    -Set 3 company
    '''
    token = login ()
    url = "http://127.0.0.1:8000/api/company/"
    headers = {
    'Content-Type': 'application/json' , 
    'Authorization' : 'Bearer ' + token
                }  
    if not token:
        print("خطا در دریافت توکن.")
        return
    companies = [
        {
            "name": "صنایع مفتول ایساتیس پویا",
            "national_id": 10860173142,
            "address": "یزد، بلوار جمهوری، نبش کوچه شرق، ساختمان بورس",
            "telephone": "03535220088",
            "registration_number": 8301 ,
            "website": "http://www.ipmill.isatispooya.com",
            "Logo": None,
            "symbol": "fevisa",
            "register_capital": 7320667814000
        },
        {
            "name": "بیمه زندگی ایساتیس پویا",
            "national_id": 14005051363,
            "address": "یزد، بلوار جمهوری، نبش کوچه شرق، ساختمان بورس",
            "telephone": "03535220088",
            "registration_number": 0000,
            "website": "http://www.isatisins.isatispooya.com",
            "Logo": None,
            "symbol": "devisa",
            "register_capital": 2400000000
        },
        {
            "name": "سبدگردانی ایساتیس پویا",
            "national_id": 14007805556,
            "address": "یزد، بلوار جمهوری، نبش کوچه شرق، ساختمان بورس",
            "telephone": "03535220088",
            "registration_number": 13702,
            "website": "http://www.sabad.isatispooya.com",
            "Logo": None,
            "symbol": "sabad",
            "register_capital": 500000000000
        }
    ]


    for j in companies :
            peyload = json.dumps(j)
            result  = requests.post(url=url , headers=headers , data=peyload)
            print('-'*25)
            print (result.content)
            print(j)



def nctouserid (nc):
    

    url = "http://127.0.0.1:8000/api/usernationalcode/"
    
    payload = json.dumps({
    "national_code": nc
    })
    headers = {
    'Content-Type': 'application/json'
    }
    try :
        response = json.loads(requests.request("GET", url, headers=headers, data=payload).text)[0]['id']
    except :
        response = None

    return response
     


def alluser(user):

    '''
    -For shareholder's function  read user according to  national code & id
    '''

    token = login()
    url = f"http://127.0.0.1:8000/api/user/"
    payload = json.dumps({"username": user})
    headers = {
    'Content-Type': 'application/json' , 
    'Authorization' : 'Bearer ' + token
                }
    try:
        response = json.loads(requests.request("GET",url, headers=headers ,data=payload).text)
        datafr = pd.DataFrame(response)
        filtered_datafr = datafr[['id', 'national_code']]
    except:
        filtered_datafr = pd.DataFrame(columns=['id', ''])  

    return filtered_datafr


def create_shareholder () :

    '''
    -Gets data from farasahm2 (registerNoBours)
    -Matching common fields :
        تعداد سهام=> amount
        کد ملی  => national_code
        symbol => company
    -Delete another fields
    -Read users from  all user's function  acording to national code
    -Change  and update company id acording to Company's function (change id for symbol)
    '''

    token = login()
    client = pymongo.MongoClient('mongodb://192.168.11.13:27017')
    url = "http://127.0.0.1:8000/api/shareholder/"   
    headers = {
    'Content-Type': 'application/json' , 
    'Authorization' : 'Bearer ' + token
                } 
    if not token:
        print("خطا در دریافت توکن.")
        return
    
    db = client['farasahm2']
    collection = db['registerNoBours']
    data = collection.find({},{'_id':0})
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset='کد ملی')
    df = df.drop(columns=['rate' ,'تاریخ تولد','بانک','کدبورسی','شماره حساب','شماره شناسنامه','نام و نام خانوادگی','نام پدر','شماره تماس','date','صادره'])
    df = df.rename(columns={'کد ملی': "national_code"})
    df['national_code'] = df['national_code'].apply(str)
    user_data = alluser('')
    user_data['national_code'] = user_data['national_code'].apply(str)
    user_data = user_data.set_index('national_code')
    df = df.set_index('national_code')
    df = df.join(user_data)
    df = df.dropna ()
    df = df[df['تعداد سهام']>0]
    df = df.reset_index()
    df = df.drop(columns='national_code')
    df['symbol'] = df['symbol'].replace('fevisa', 1).replace('sabad', 3).replace('devisa', 2)
    df = df[(df['symbol'] == 1) | (df['symbol'] == 3) | (df['symbol'] == 2)]
    df['id'] = df ['id'].apply(int)
    df['تعداد سهام'] = df ['تعداد سهام'].apply(int)
    df = df.rename(columns={'id': "user"})
    df = df.rename(columns={'تعداد سهام': "amount"})
    df = df.rename(columns={'symbol': "company"})
    df = df.to_dict('records')


    for shareholder in df:
        payload = json.dumps(shareholder)
        try:
            result = requests.post(url=url, headers=headers, data=payload)
            if result.status_code == 201:
                print("سهامدار با موفقیت ایجاد شد:", shareholder)
            else:
                print("خطا در ایجاد سهامدار:", result.content)
        except requests.RequestException as e:
            print("خطا در درخواست:", e)



def  create_customer () :

    '''
    -Create customer acording to user , company id 
    -Change  and update user , company id  
    '''

    token = login()
    url = "http://127.0.0.1:8000/api/customer/"
    headers = {
    'Content-Type': 'application/json' , 
    'Authorization' : 'Bearer ' + token
                } 
    if not token:
        print("خطا در دریافت توکن.")
        return
    
    customer = [
        {
        "user":3 , 
        "company" :3 ,
        },
        {
        "user":4 , 
        "company" :2 ,
        },
        {
        "user":5 , 
        "company" :1 ,
        },
        {
        "user":3 , 
        "company" :1,
        },

    ]


    for j in customer :
            peyload = json.dumps(j)
            result  = requests.post(url=url, headers=headers, json=j)
            print('-'*25)
            print (result.content)
            print(j)



def userpermissions () :

    '''
    -Create endpoint to name urls
    '''

    token = login()
    url = "http://127.0.0.1:8000/api/permission/"
    headers = {
  'Content-Type': 'application/json' , 
  'Authorization' : 'Bearer ' + token
                }
    if not token:
        print("خطا در دریافت توکن.")
        return
    
    name_url = [
    {'endpoint': 'captcha'},
    {'endpoint': 'otp'},
    {'endpoint': 'login'},
    {'endpoint': 'companies'},
    {'endpoint': 'companieswithemployees'},
    {'endpoint': 'customer'},
    {'endpoint': 'shareholder'},
    {'endpoint': 'user'},
    {'endpoint': 'usernationalcode'},
]


    for i in name_url:
        payload = json.dumps(i)
        try:
            result = requests.post(url=url, headers=headers, data=payload)
            print('-' * 25)
            print(result.text)
        except requests.exceptions.RequestException as e:
            print(f"Payload: {payload}")

    

def groups () :

    '''
    -Create  postion group and postion 
    -Get premission (endpoint) 
    -Create access acordinng to postion name and endpoint
    -Maybe need  check  presmision' id 
    '''

    token = login()
    url = "http://127.0.0.1:8000/api/groups/"
    headers = {
    'Content-Type': 'application/json' , 
    'Authorization' : 'Bearer ' + token
                }
    
    if not token:
        print("خطا در دریافت توکن.")
        return
        
    result = requests.get(url = "http://127.0.0.1:8000/api/permission/" , headers=headers)
    print (result)
    result = json.loads(result.content)

    endpoint_name = {}
    for j in result:
        if 'endpoint' in j and 'id' in j:
            endpoint_name[j['endpoint']] = j['id']
        else:
            print(f"عنصر {j} دارای کلیدهای لازم نیست و اضافه نشد.")
    print(endpoint_name)

    groups = [
    {"name": "مدیر سیستم",
    "endpoint":[endpoint_name['captcha'],
                endpoint_name['otp'],
                endpoint_name['login'],
                endpoint_name['user'],
                endpoint_name['usernationalcode'],
                endpoint_name['companies'],
                endpoint_name['companieswithemployees'],
                endpoint_name['customer'],
                endpoint_name['shareholder']] },

    {"name": "پرسنل",
    "endpoint":[endpoint_name['captcha'],
                endpoint_name['otp'],
                endpoint_name['login'],
                endpoint_name['user'],
                endpoint_name['usernationalcode'],
                endpoint_name['companies'],
                endpoint_name['companieswithemployees']] },

    {"name": "سهامدار",
    "endpoint":[endpoint_name['captcha'],
                endpoint_name['otp'],
                endpoint_name['login'],
                endpoint_name['user'],
                endpoint_name['usernationalcode'],
                endpoint_name['shareholder']]},

    {"name": "مشتری",
    "endpoint":[endpoint_name['captcha'],
               endpoint_name ['otp'],
               endpoint_name ['login'],
               endpoint_name ['user'],
               endpoint_name ['customer']] },
    ]

    for i in groups:
        payload = json.dumps(i)
        try:
            result = requests.post(url=url, headers=headers, data=payload)
            print('-' * 25)
            print(result.content)
        except requests.exceptions.RequestException as e:
            print(f"Payload: {payload}")


def customer_remain():
    
    '''
    -Create  customer remain's field 
    -Get customer  acording to id  (id)
    -Customer is uniqe
    -Other feilds set random 
    -Maybe need  check  (customer , user , company)'s id 
    '''
    token = login()
    if not token:
        print("خطا در دریافت توکن.")
        return
    
    url_customer_remain = "http://127.0.0.1:8000/api/customerremain/"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }

    response = requests.get(url="http://127.0.0.1:8000/api/customer/", headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        
        if isinstance(data, list):
            customer_ids = [item.get("id") for item in data if "id" in item]
            
            if not customer_ids:
                print("هیچ شناسه مشتری در پاسخ یافت نشد.")
                return
        else:
            print("پاسخ API انتظار می‌رفت یک لیست باشد.")
            return
    else:
        print(f"Request failed with status code {response.status_code}")
        return

    customer_remain_data = [
        {"id": customer_id, "other_key": "value1"} for customer_id in customer_ids
    ]
    customer_remain_data = [
            {
                "customer": customer_id,
                "adjusted_remain": int(random.uniform(10, 50000)  ),
                "blocked_remain": int(random.uniform(10, 10000)  ),
                "credit": int(random.uniform(10, 100000000)  ),       
                "current_remain": int(random.uniform(10, 6000)  ),
            } for customer_id in customer_ids
        ]

    for i in customer_remain_data:
        payload = json.dumps(i)
        try:
            result = requests.post(url=url_customer_remain, headers=headers, data=payload)
            print('-' * 25)
            print(result.content)
        except requests.exceptions.RequestException as e:
            print(f"Payload: {payload}")
            print(f"Error: {e}")


    ''' 
    -Run functions respectively Up to Down 
    -Always check id maybe changed 
    '''
# read()
# folan()
# create_company()
# create_shareholder()
# create_customer ()
# userpermissions ()
# groups ()
# customer_remain()
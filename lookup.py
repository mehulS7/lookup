from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup as soup
from datetime import datetime
import re
import pandas as pd
from stdiomask import getpass
import pyodbc as dbc
import json
import os
import requests

config_present = os.path.exists('config.json')
if config_present:
    with open('config.json','r') as f:
        config_data = json.load(f)

if config_present:
    path = config_data['webdriver_path']
    username = config_data['linkedin_username']
    password = config_data['linkedin_pass']
    keywords = config_data['keywords']
    location = config_data['location']
else:
    path =  input('\nEnter the Chrome Web Driver Path\n>').strip()
    username = input('\nEnter LinkedIN UserName:\n>').strip()
    password = getpass(prompt='\nEnter LinkedIN Password:\n>',mask='*').strip()
    keywords = input('\nEnter the keywords for whom you want the posts extracted. Please separate each of them using semicolon (;).\nEx: teacher;artist;developer\n>').strip().split(';')
    location = input('\nEnter the location. Use the same method as above to separate.\nEx: new york;london;bangalore\n>').strip().split(';')


df = pd.DataFrame(columns=['Date','Search_Element','Title','Content','URL'])
current_dt = datetime.now().date()
elems = [key.strip() + ' ' + loc.strip() for key in keywords for loc in location]

query_create_table = """
                    if not exists (select table_name from information_schema.tables where table_name = 'lookup')
                    create table lookup (Date Date,
                                        Search_Element varchar(100),
                                        Title varchar(100),
                                        Content varchar(max),
                                        URL varchar(max)
                                        )
                    """

query_insert_table = 'insert into lookup values (?,?,?,?,?)'

query_filter_table = '''
                    with cte
                    as
                    (select *,row_number() over (partition by URL order by Date) as rk from lookup)
                    delete from cte where rk > 1
                    '''

query_select_table = "select * from lookup where Date = '"+current_dt.strftime('%Y-%m-%d')+"'"


def hitSearch(e):
    time.sleep(2)
    search = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.TAG_NAME,'input')))
    search.clear()
    search.send_keys(e)
    search.send_keys(Keys.RETURN)


def extractInfo(e):
    global df
    try:
        post_src = soup(driver.page_source,'html.parser')
        section = post_src.find('section',{'class':'scaffold-layout__detail search-marvel-srp__content-detail-detail'})
        if section.find('div',{'class':'feed-shared-update-v2__description-wrapper ember-view'}):
            post_content = section.find('div',{'class':'feed-shared-update-v2__description-wrapper ember-view'}).text
        else:
            post_content = "**No Text Found**"
        clean_content = re.sub('\n|\xa0|\u200b',' ',post_content).strip()
        post_title = section.find('span',{'dir':'ltr'}).text
        flex_catch = driver.find_element_by_class_name('search-marvel-srp__content-detail-item')
        top_bar = section.find('div',{'class': 'feed-shared-actor display-flex feed-shared-actor--with-control-menu'})
        if top_bar.find('li-icon',{'class':{'artdeco-button__icon'}}):
            flex_catch.find_elements_by_tag_name('button')[3].click()
        else:
            flex_catch.find_elements_by_tag_name('button')[2].click()
        time.sleep(1)
        flex_elem = flex_catch.find_element_by_class_name('artdeco-dropdown__content-inner')
        flex_elem.find_elements_by_tag_name('li')[1].click()
        post_link = pd.read_clipboard().columns.values[0]
        df = df.append({'Date' : current_dt, 'Search_Element': e ,'Title' : post_title, 'Content': clean_content, 'URL': post_link},ignore_index=True)
        print(f'Post: {df.shape[0]} extracted')
    except:
        print('Extraction failed due to slow speed.')


def traversePosts(n,e):
    for i in range(1,n+1):
        post = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.XPATH,'//*[@id="search-marvel-srp-scroll-container"]/div/div[1]/ul/li[' + str(i) + ']')))
        post.find_element_by_tag_name('div').click()
        time.sleep(1)
        extractInfo(e)


def extractElem(e):
    page = 1
    time.sleep(2)
    while(page):
        if page != 1:
            link = driver.current_url+'&page='+str(page)
            driver.get(link)
            time.sleep(3)
        pg_src = soup(driver.page_source,'html.parser')
        n = len(pg_src.findAll('li',{'class':'reusable-search__result-container'}))
        print(f'\nPosts: {n}\t\t||\t\tPage: {page}\t\t||\t\tSearch Element: "{e}"')
        if n!= 0:
            traversePosts(n,e)
            page +=1
        else:
            break


def saveToDB(db_driver,db_server,db_database,db_trusted,db_username=None,db_pass=None):
    global df
    try:
        if db_trusted == 'y':
            conn = dbc.connect(driver='{'+db_driver+'}',server=db_server,database=db_database,trusted_connection = 'yes')
        else:
            conn = dbc.connect(driver='{'+db_driver+'}',server=db_server,database=db_database,trusted_connection = 'no',UID = db_username,PWD = db_pass)
        cursor = conn.cursor()
        cursor.execute(query_create_table)
        print('\n-Table lookup created, if not existed.')
        df_tuples = [(row.Date,row.Search_Element,row.Title,row.Content,row.URL) for row in df.itertuples()]
        cursor.executemany(query_insert_table,df_tuples)
        print('\n-Records Inserted.')
        cursor.execute(query_filter_table)
        print('\n-Records de-duped.')
        cursor.commit()
        df = pd.read_sql(query_select_table,conn).copy()
        return 0
    except:
        print('\nInjection Failed.')
        return 1


def saveToTelegram(api_key,chat_id):
    try:
        df['Index'] = df.index + 1
        df.to_html('lookup_'+current_dt.strftime('%Y_%m_%d')+'.html',justify='left',index=False,columns=['Index','Search_Element','Title','Content','URL'],render_links='URL',col_space=[50,120,120,400,50],border=4)
        f = {'document' : open('lookup_'+current_dt.strftime('%Y_%m_%d')+'.html','rb')}
        res = requests.post('https://api.telegram.org/bot'+api_key+'/sendDocument?chat_id='+chat_id,files=f)
        if res.status_code == 200:
            print('\n-File sent to the BOT.')
            return 1
        else:
            print('File tansfer failed.')
            return 0
    except:
        print('File transfer failed.')
        return 1


def saveToExcel():
    df.to_excel('lookup_'+current_dt.strftime('%Y_%m_%d')+'.xlsx',index=False)
    print(f'\n-File: {"lookup_"+current_dt.strftime("%Y_%m_%d")} saved as excel.')


driver = webdriver.Chrome(path)
driver.get('https://www.linkedin.com')
driver.maximize_window()

try:
    submit = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.XPATH,'//*[@id="main-content"]/section[1]/div[2]/form/button')))
    driver.find_element_by_id('session_key').send_keys(username)
    driver.find_element_by_id('session_password').send_keys(password)
    submit.click()
    try:
        time.sleep(1)
        WebDriverWait(driver,10).until(EC.presence_of_element_located((By.XPATH,'//*[@id="remember-me-prompt__form-primary"]/button'))).click()
    except:
        print()
except:
    print('Slow Internet connection. Timeout at 10 seconds.')


try:
    for idx,e in enumerate(elems):
        hitSearch(e)
        if idx == 0:
            top_bar = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CLASS_NAME,'search-reusables__filters-bar-grouping')))
            for i in range(7):
                if top_bar.find_elements_by_tag_name('li')[i].text == 'Posts':
                    top_bar.find_elements_by_tag_name('li')[i].click()
                    break
            posted_dt = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.XPATH,'//*[@id="search-reusables__filters-bar"]/ul/li[4]')))
            posted_dt.click()
            if config_present:
                date_posted = config_data['date_posted']
            else:
                date_posted = input('\nWhat is the period you would want to filter out the posts. "Day" or "Week"?\n>').strip().lower()
            driver.implicitly_wait(5)
            if date_posted == 'day':
                posted_dt.find_elements_by_tag_name('li')[0].find_element_by_tag_name('p').click()
            elif date_posted == 'week':
                posted_dt.find_elements_by_tag_name('li')[1].find_element_by_tag_name('p').click()
            posted_dt.find_elements_by_tag_name('button')[1].click()
        extractElem(e)
except:
    print('Something went wrong.')
finally:
    driver.quit()
    fail = 0
    df = df[~(df.Content == '**No Text Found**')].drop_duplicates(subset='Content').append(df[df.Content == '**No Text Found**'])
    # df.reset_index(inplace = True)
    # df.drop('index',axis=1,inplace=True)
    print(f'\nPosts found for each category:\n{df["Search_Element"].value_counts()}')
    if df.shape[0] != 0 :
        if config_present:
            all_save = config_data['saved_areas']
        else:
            all_save = input('\nWhere would you like to save the result -> DataBase: "D", Excel: "E" or receive on Telegram: "T".\nTo save on multiple entries, separate the inputs via semicolon (;). Ex: d;e;t\n>').lower().split(';')
        if 'd' in all_save:
            if config_present:
                db_driver = config_data['db_driver']
                db_database = config_data['db_database']
                db_server = config_data['db_server']
                db_trusted = config_data['db_trusted']
                db_username = config_data['db_username']
                db_pass = config_data['db_pass']
            else:
                print('\nPlease Enter the Database details accurately.')
                print(f'\nList of drivers available\n{dbc.drivers()}')
                db_driver = input('\nEnter the Driver Name:\n>')
                db_server = input('\nEnter the Server Name:\n>')
                db_database = input('\nEnter the Database Name:\n>')
                db_trusted = input('\nWould you like to go ahead with windows authentication. "Y" or "N"\n>').lower()
                db_username = ''
                db_pass = ''
                if db_trusted == 'y':
                    fail += saveToDB(db_driver,db_server,db_database,db_trusted)
                elif db_trusted == 'n':
                    db_username = input('\nEnter DataBase UserName:\n>')
                    db_pass = getpass(prompt='\nEnter DataBase Password:\n>',mask='*')
                    fail += saveToDB(db_driver,db_server,db_database,db_trusted,db_username,db_pass)
                else:
                    print('\nWrong Input.')
        if 't' in all_save:
            if config_present:
                api_key = config_data['api_key']
                chat_id = config_data['chat_id']
            else:
                api_key = getpass(prompt='\nEnter Telegram API Key:\n>',mask='*').strip()
                chat_id = getpass(prompt='\nEnter Telegram Bot Chat ID:\n>',mask='*').strip()
            fail += saveToTelegram(api_key,chat_id)
            try:
                os.remove('lookup_'+current_dt.strftime('%Y_%m_%d')+'.html')
            except:
                print(f"\nAutomatic deletion of {'lookup_'+current_dt.strftime('%Y_%m_%d')+'.html'} failed.")
        if 'e' in all_save or len(set(all_save) - set(['e'])) == fail:
            saveToExcel()
        if (~config_present):
            save_config = input('\nWould you like to save all the inputs in a config file. "Y" or "N"\n>').lower()
            if save_config == 'y':
                config_data = {'webdriver_path' : path,'linkedin_username' : username,'linkedin_pass' : password,'keywords' : keywords,'location': location,
                'db_username' : db_username, 'db_pass': db_pass,'db_server': db_server,'db_driver': db_driver, 'db_database': db_database,'db_trusted' : db_trusted ,'api_key' : api_key, 'chat_id':chat_id,
                'saved_areas' : all_save}
                with open('config.json','w') as f:
                    json.dump(f,'config.json')
                print('\n-Config file saved.')

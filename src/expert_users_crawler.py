'''
Created on Mar 25, 2011

@author: kykamath
'''
import cjson, json, sys
from lxml.html import parse
from collections import defaultdict

twitter_users_folder = '/mnt/chevron/kykamath/data/twitter/odds_maker/users/'
twitter_users_crawl_folder = twitter_users_folder + 'crawl/'

#class API:
#    @staticmethod
#    def parse(url):
#        doc = None
#        try:
#            while doc == None: 
#                doc = parse(url).getroot()
#                print url
#        except: return None
#        return doc 
    
def scrapeListsForUser(user):
    class List:
        def __init__(self, uri, member_count): self.uri, self.member_count = uri, member_count
    doc = None
    while doc == None: 
        url = 'http://twitter.com/%s/lists/memberships'%user
        doc = parse(url).getroot()
    for row in doc.cssselect('table.users-lists tr'): 
        member_count = row.cssselect('td.count')[0].text.split()[-1]
        uri = row.cssselect('span.list-info a')[0].get('href')
        yield List(uri, member_count)
        
def scrapeMembersForList(list):
    class Member:
        def __init__(self, screen_name, id): self.screen_name, self.id = screen_name, int(id)
    doc = None
    while doc == None: 
        url = 'http://twitter.com%s/members'%list
        doc = parse(url).getroot()
        for k in zip([row.cssselect('address span a')[0].get('href').split('/')[-1] for row in doc.cssselect('td.user-detail')], [row.attrib['id'].split('_')[-1] for row in doc.cssselect('tr') if 'id' in row.attrib]):  yield Member(k[0], k[1])

class UsersCrawl:
    
    topics = defaultdict(dict)
    topics_file = 'snowball_seeds'
    
    users_to_crawl_file = twitter_users_crawl_folder+'users_to_crawl.json'
    lists_to_crawl_file = twitter_users_crawl_folder+'lists_to_crawl.json'
    crawled_info_file = twitter_users_crawl_folder+'crawled_info.json'
    
    users_file = twitter_users_crawl_folder+'users'
    lists_file = twitter_users_crawl_folder+'lists'

    # Crawl conditions.
    min_followers_count = 250
    max_member_count = 20
    number_of_items_to_crawl_every_run = 10
    
    number_of_lists_per_user_limit = 100
    time_to_sleep_after_every_api_call = 3
    
    
    @staticmethod
    def loadSeedInformation():
        for l in open(UsersCrawl.topics_file):
            data = l.strip().lower().split()
            UsersCrawl.topics[data[1]][data[0]] = data[2:]
    
    @staticmethod
    def getListsFor(user, topic):
        returnSet = set()
        for item in scrapeListsForUser(user):
            slug = item.uri.split('/')[-1]
            for keyword in UsersCrawl.topics[topic]['keywords']: 
                if slug.find(keyword) > -1: returnSet.add(item.uri.lower())
        return returnSet
    
    @staticmethod
    def getUsersFor(uri):
        returnSet = set()
        for item in scrapeMembersForList(uri):
            returnSet.add((item.screen_name.lower(), item.id))
        return returnSet
    
    @staticmethod
    def appendAsJsonToFile(data, file):
        f = open(file, 'a')
        f.write(cjson.encode(data)+'\n')
        f.close()
        
    @staticmethod
    def crawl():
        UsersCrawl.loadSeedInformation()
        usersToCrawl = json.load(open(UsersCrawl.users_to_crawl_file))
        listsToCrawl = json.load(open(UsersCrawl.lists_to_crawl_file))
        crawledInfo = json.load(open(UsersCrawl.crawled_info_file))
        while True:
            for topic in usersToCrawl:
                crawlList=usersToCrawl[topic][:UsersCrawl.number_of_items_to_crawl_every_run]
                for userTuple in crawlList:
                    user = userTuple[0]
                    try:
                        lists = UsersCrawl.getListsFor(user, topic)
                        usersToCrawl[topic].remove(userTuple)
                        crawledInfo['users'][user]=True
                        UsersCrawl.appendAsJsonToFile({'id': userTuple[1], 's': user, 't': topic, 'l': list(lists)}, UsersCrawl.users_file)
                        for l in lists: 
                            if l not in crawledInfo['lists'] and l not in listsToCrawl[topic]: listsToCrawl[topic].append(l)
                    except: pass
            for topic in listsToCrawl:
                crawlList=listsToCrawl[topic][:UsersCrawl.number_of_items_to_crawl_every_run]
                for uri in crawlList:
                    try:
                        users = UsersCrawl.getUsersFor(uri)
                        listsToCrawl[topic].remove(uri)
                        crawledInfo['lists'][uri]=True
                        UsersCrawl.appendAsJsonToFile({'id': uri, 't': topic, 'u': list(users)}, UsersCrawl.lists_file)
                        for userTuple in users: 
                            user = userTuple[0]
                            if user not in crawledInfo['users'] and userTuple not in usersToCrawl[topic]: usersToCrawl[topic].append(userTuple)
                    except: pass
                        
            json.dump(usersToCrawl, open(UsersCrawl.users_to_crawl_file, 'w'), separators=(',',':'))
            json.dump(listsToCrawl, open(UsersCrawl.lists_to_crawl_file, 'w'), separators=(',',':'))
            json.dump(crawledInfo, open(UsersCrawl.crawled_info_file, 'w'), separators=(',',':'))

def run():
    if sys.argv >= 2:
        if sys.argv[1] == 'users_crawler': UsersCrawl.crawl()
        
if __name__ == '__main__':
    run()

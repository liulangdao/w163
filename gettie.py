#!/usr/bin/env python3
import threading
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import sys
import time
import urllib.request
from multiprocessing import Pool
import datetime
from bson.objectid import ObjectId

def gettie(url):	
	container = {}
	dcap = DesiredCapabilities.PHANTOMJS
	dcap["phantomjs.page.settings.loadImages"] = False	
	dcap["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"	
	driver = webdriver.PhantomJS(desired_capabilities=dcap)
	driver.set_page_load_timeout(20)								
	container['url']=url
	container['hash']=hash(url)
	ties = []							
	try:
		driver.get(url)
		container['title']=driver.find_element_by_id("epContentLeft").find_element_by_tag_name("h1").text
		container['content']=driver.find_element_by_id("endText").text
		container['postTotal'] = int(driver.find_element_by_xpath("//div[@class='post_tie_top']").text)
		plast = re.search('\w+\.html$',url).group()
		pattern = 'http://comment.+?'+plast      
		suburl=re.search(pattern,driver.page_source).group()
		driver.get(suburl)	
		divs = driver.find_elements_by_xpath("//div[@class='reply essence']")	
		for i in range(len(divs)):			
			ties.append(divs[i].text)			
		print(len(ties))
		driver.quit()
	except:
		driver.quit()
		with open('log.txt','a') as f:
			f.write(url+'\n')
		print(url)
	client = MongoClient('localhost',27017)
	db = client.w163
	w163 = db.w163
	getContent = {'content':container['content'],'postTotal':container['postTotal'],'comments':ties}
	data = {'hash':container['hash'],'date':int(time.strftime('%Y%m%d%H%M%S')),'url':container['url'],'title':container['title'],'article':getContent}
	w163.insert(data)


def getalllink():
	url = 'http://www.163.com'
	links = set()
	html = urllib.request.urlopen(url,timeout=30)
	bsObj = BeautifulSoup(html,'lxml')
	geta = bsObj.findAll('a',href=re.compile("^(http://)[a-zA-Z]+(.163.com/)$"))
	for link in geta:
		if link.attrs['href'] is not None:
			if link.attrs['href'] not in links:
				links.add(link.attrs['href'])
	allLinks = getlinks(links)
	return allLinks

def getlinks(links):							
	client = MongoClient('localhost',27017)
	db = client.w163
	w163 = db.w163
	gen_time = datetime.datetime.today() - datetime.timedelta(15)
	dummy_id = ObjectId.from_datetime(gen_time)
	print(dummy_id)
	urls = [i['url'] for i in w163.find({"_id":{"$gt":dummy_id}},{'url':1})]		
	print(len(urls),urls[0])	
	allLinks = set()
	for subLink in links:
		try:
			html = urllib.request.urlopen(subLink,timeout=10)
			bsObj = BeautifulSoup(html,'lxml',from_encoding='gb18030')
			for url in bsObj.findAll("a",href=re.compile("^(" + subLink + ")\d+.+(html)$")):
				if url.attrs['href'] not in urls:			
					allLinks.add(url.attrs['href'])		
		except:
			pass
	return allLinks


if __name__ == "__main__":
	allLinks = list(getalllink())
	print(len(allLinks))
	loops = range(len(allLinks))
	print('start')
	pool = Pool(5)
	for i in loops:				
		pool.apply_async(gettie,(allLinks[i],))
	pool.close()
	pool.join() 

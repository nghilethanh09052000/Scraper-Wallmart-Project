import scrapy
import string
import json
from ..utils import utils, COOKIE_STRING
import pandas as pd

class IndexSpider(scrapy.Spider):

    name = "index"

    def start_requests(self):

        data = pd.read_csv('D:\Study\Python\Crawl Data Project\Scraper_Wallmart_Project\wallmart\wallmart\spiders\categories.csv')
        category_urls = data['Url'].values.tolist()


        for category_url in category_urls:
            yield scrapy.Request(
                url = category_url,
                callback = self.get_all_product,
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'cookie': COOKIE_STRING,
                    'user-agent': utils.random_user_agent()
                }
            )
    
    def get_all_product(self, response):
        script_tag  = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        if script_tag is None: return

        json_blob = json.loads(script_tag)
        product_list = json_blob["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"]
        for product in product_list:
            walmart_product_url = 'https://www.walmart.com' + product.get('canonicalUrl', '').split('?')[0]
            yield scrapy.Request(
                url = walmart_product_url, 
                callback = self.parse_product_data,
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'cookie': 'pxcts=766481ae-215d-11ee-aab6-715a644b7858; _pxvid=766479a9-215d-11ee-aab6-f6c42362e100; ACID=c1958658-86e3-446c-a85e-dc9ac8abc687; hasACID=true; vtc=dRaC12wRArBprrKclixaFI; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1689240044635; assortmentStoreId=3081; hasLocData=1; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY4OTIzOTU3OTY5NCwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sInNoaXBwaW5nQWRkcmVzcyI6eyJ0aW1lc3RhbXAiOjE2ODkyMzk1Nzk2OTQsInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSIsInRpbWVzdGFtcCI6MTY4OTIzOTU3OTY5Mywic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCIsInNlbGVjdGlvblNvdXJjZSI6bnVsbH1dfSwicG9zdGFsQ29kZSI6eyJ0aW1lc3RhbXAiOjE2ODkyMzk1Nzk2OTQsImJhc2UiOiI5NTgyOSJ9LCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6YzE5NTg2NTgtODZlMy00NDZjLWE4NWUtZGM5YWM4YWJjNjg3In0%3D; bstc=bM914mURvczU5a6Z0lVlAY; mobileweb=0; xpth=x-o-mart%2BB2C~x-o-mverified%2Bfalse; xpa=2NYvd|33xqR|4Gf2Q|6JxVP|6cfXU|8r3rK|9AV1f|BukPC|Cx7X5|IEJbo|IRAHO|IhmrE|KvYZX|O-4-6|T93o8|TmQJ_|Za7p9|ZllwQ|_4HRC|_NzN8|_uNDy|cRnHN|dSU--|dayNl|i9CG3|kKjwb|klYmM|pyVOq|qkVpx|v_9TH; exp-ce=1; exp-ck=33xqR26JxVP16cfXU18r3rK19AV1f1BukPC1Cx7X51IEJbo1IRAHO1KvYZX1O-4-61TmQJ_2_NzN81_uNDy1cRnHN1klYmM1qkVpx1v_9TH1; xptc=assortmentStoreId%2B3081; QuantumMetricUserID=224cc5ac592a94c011db94bfb8ac81f9; xpm=1%2B1689258950%2BdRaC12wRArBprrKclixaFI~%2B0; __cf_bm=xE0BK8QN0OkJUzDFpB5wrLWo0P_s_2mZ6Fso58lQ1fs-1689260525-0-AXBiB6QtCsF9cxFP8x4SfqhK2B4UChjmLb4rn4nx8/eyv/CuRSV7pbNX3zFjprAisBRR1ukmh9Fqm/+jQGQlK0CvmwFTkcAbdWakE1mgIYdF; auth=MTAyOTYyMDE4WPV6m7nAijmpLAmX3KofCE3IEu9JM4Ooa%2BA1RkPYL6mIilYiYiRkaHE0c7nYTKTaTBOYh6I56FIon%2FlL7TZnIvS8H7%2BMIIhMSFUESjfXLc6mNkTX6QB9XoC8MerAqyi6767wuZloTfhm7Wk2KcjygobRHThsmZk%2BGcqTfIab85S5%2FwJ3APrQdhpASr%2Bm1rMFfG%2BEPPeSXk2uTJCtt35mJ7s4%2FF4T1bH0K%2B6qiIolM7cUMk70P8glgOEpLOprhDfMDCcb9mgycy9jtT1uIyOBHa%2FWeHLdaOIRPuPDY3aiRAGS13mIrNARC%2FUWzirWf4LJ5aYHc%2BoTb9Zdg9o871wWMOuGN9wYYrppIndnk6EaoMd19bAc6F1fmC8IaXhaeSqnz%2F4HUePCNlQXuJnTF%2FJyMkjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9JTlNUT1JFIiwiUElDS1VQX0NVUkJTSURFIl0sInNlbGVjdGlvblR5cGUiOiJMU19TRUxFQ1RFRCJ9XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjozOC40NzQ2LCJsb25naXR1ZGUiOi0xMjEuMzQzOCwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2V9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJpbnRlbnQiOiJQSUNLVVAifSwiaW5zdG9yZSI6ZmFsc2UsImRlbGl2ZXJ5Ijp7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR2VyYmVyIFJvYWQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjMwODEiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl0sInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQifSwicmVmcmVzaEF0IjoxNjg5MjY0Njc0MzIxLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6YzE5NTg2NTgtODZlMy00NDZjLWE4NWUtZGM5YWM4YWJjNjg3In0%3D; ak_bmsc=7B61F1E63778197ED641EEAEE3F3B90F~000000000000000000000000000000~YAAQ7FJNGyHx7E6JAQAAvnTUTxRwoDy/78/tWEr79n3TFeyJbf9Tdmoj8jwEptP0h80/sz+ILeVXkRm/Ej6utriqJ1O4jQCjJktcAFZwNj60V8bBRr3YfmQYMK6F5UjDGqImWdhrQTN9bD6/SWtGMn+AQkc0oLjb03817K3NGpenNrcG+3S66x9ioJndMLnyjXY2mgwpCM1GV8sO28mMGgWwi4Ivkw9KNSqMpGtP2txFMdU623AQ5AYSxcCaLh3VOvEORoc14QdJYgNVLSzkHe5NGMFsm8uBZ0m1JmAdadOs00s42CDjZRe8w+5vrzSZTv9SaFuutnTUQUBQKPYZSkzh1k442IgqRvp7Fg7CbGGRkyH5BHcirWjzw6vdTLwAlVa3/QunqyTNC1dD6oug0DwI+icuFHKWbr/fJeVTN1QBB6jXHuW7sOmjWR5uFaoq51VubMQuhcY3L/nKGikTixQCsD9oXHpowX9igcgrCg4=; _px3=4c289d78b0e6906b19dcf4bf803dfeb7f07b5f8cb47dad9c6dfc2fa271ec3364:qlJP04pNKb1fhiFOnOwKZM1I+OeoFy6Q2zKUpKb/beNHN96INKuLgiwziwPBlzlhQNNJOYmefkYKskktZv5zng==:1000:98+2lnDGBFbqd6If66IgHRR/lCrW/rEPZQKQhBRnepfSXGCfWNhjDCKeVVY5h/xSguB4TazVMFlXnMjF/Hh9w0a3uyqkkPTGa+6niQmFwxafMQ1hwsnZnA+bCAT3jI48Vb7e5Vz+ZBwWbz7BgWlOf+M0mJlBZ3uZ7MPrnYJ1VRbYcY4uxkBKoals1oq35voYoI6phRqr0QqTA4PTs1ViWQ==; bm_mi=D7DC3BF971749F0F20E0BE8A6A32C606~YAAQ7FJNG3Tx7E6JAQAArozUTxRSpZMGo27A+lJJzWdDpNMcOba2R56h3rJ4zGNe33YXeKm+y+RpLkNbzCW/X+DlR2Oj3VsiItykYnze1GhiXbKWD0F6VqRiSW80V97Gkg8JBtGXH362oIQpnSzsC0zdjCuUhuIzvPtI0hsUd+j7uspHelJPdUupAX9exs/MEXB9L6IM6S6FmbXBmM+05v5hPX2SmuPeIMrqlWQbkiMoWajBZ5W6hCVoLtQguNkQk+19KPC1LIv71otR0uPn8ez1zL87p9jisjS1/bO8FA5N5yDKvliD+cRh0zJOezBFVDqhpus=~1; TS01a90220=0178545c907a3a78ee4957328c09dbb4e373e099e869dccec18b5406154348135b48b3800b02ba575c7537db243fac8d650a49a089; TS2a5e0c5c027=087360e19aab2000a5c92191fd9248e1a6f5f94e8a9aabb2cc9a0bdc8877666db5f12d9f6a0339c608903b91851130008f61337152d076e8c2359cf591c67def4b461066adbb2de6db1eb27d7c9f4246d7b2701f71afb0cfe607bb9ef3ea379b; _pxde=50af173301f365e4ae400de5d814ef176a774bfae8341163854649347ffcd55c:eyJ0aW1lc3RhbXAiOjE2ODkyNjE0Nzg4MDN9; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1689261481000@firstcreate:1689239739378"; xptwg=62396100:8884E5AB85D448:15B19FC:CA0E19CF:88CFAA48:3E09FB73:; xptwj=qq:80c29876e0ea78f10f44:TvhTX/fWVL6jUp5UOrRzhwz78MEGgSPwOCofOFvqMyMhh8s05V5tAVo/PKqV98YoMId8tWcpcAHUYd/M1dDO/cc1G+8W9Wg3o8SUlvgH5SdAgHJLdf7BW1KNB9uosrU0BLPm+xOSCy3rCwXfAfOyf7vr+xd+sW0o6TrXH3RkvFPapsPb3AXXSNZ1SA49ES52XLJz; bm_sv=A2108C4EAB3032BCB68E0BEE04B98F26~YAAQ7FJNG7fx7E6JAQAAd5/UTxT2GFUvKm98B+Km/XUiot+NNJ8CepikgF/7oUo7eEATjs93aOvF76Lrr/ARtq1X76cYsk+p1KoR05TDm3kYr4mHIhq/jC/yG4jZ1UONJFMxZoOk81Uxj9av+ivHkviTk52YRUi2UYD5vM/DvdABMwV8doQv4t7mLMs8v5Dg+zwdzrxSxCZraQ1xD64WABC/Gfx55QHxKYuEYjGl/S1xLBMlzKlciz+fGxg4Ha9lCg==~1',
                    'user-agent': utils.random_user_agent()
                },
                meta = {
                    'url' : walmart_product_url
                }
            )
           
    def parse_product_data(self, response):

        script_tag  = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()

        if script_tag is None: return

        json_blob = json.loads(script_tag)
        raw_product_data = json_blob["props"]["pageProps"]["initialData"]["data"]["product"]

        yield {
            'id':  raw_product_data.get('id'),
            'type':  raw_product_data.get('type'),
            'name':  raw_product_data.get('name'),
            'brand':  raw_product_data.get('brand'),
            'averageRating':  raw_product_data.get('averageRating'),
            'manufacturerName':  raw_product_data.get('manufacturerName'),
            'shortDescription':  raw_product_data.get('shortDescription'),
            'thumbnailUrl':  raw_product_data['imageInfo'].get('thumbnailUrl'),
            'price':  raw_product_data['priceInfo']['currentPrice'].get('price'), 
            'currencyUnit':  raw_product_data['priceInfo']['currentPrice'].get('currencyUnit'),  
            'url': response.meta['url']
        }
import logging, urllib, json, datetime, time
from collections import Counter
logging.basicConfig(level=logging.DEBUG)
from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode
from spyne import Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
#create by Miao WANG
class HelloWorldService(ServiceBase):
    @rpc(float, float, float, _returns=Iterable(Unicode))
    def checkcrime(ctx, lat, lon, radius):
        url =  "https://api.spotcrime.com/crimes.json?lat="+str(lat)+"&lon="+str(lon)+"&radius="+str(radius)+"&key=."
        dangerous_streets = dict({})
        crime_type_count = {
            "Assault" : 0,
            "Arrest" : 0,
            "Burglary" : 0,
            "Robbery" : 0,
            "Theft" : 0,
            "Other" : 0,    
        }
        event_time_count = {
            "12:01am-3am" : 0,
            "3:01am-6am" : 0,
            "6:01am-9am" : 0,
            "9:01am-12noon" : 0,
            "12:01pm-3pm" : 0,
            "3:01pm-6pm" : 0,
            "6:01pm-9pm" : 0,
            "9:01pm-12midnight" : 0,
        }
        checkTimeList = ['12:01 AM',
        '3:00 AM','3:01 AM',
        '6:00 AM','6:01 AM',
        '9:00 AM','9:01 AM',
        '12:00 AM','12:01 PM',
        '3:00 PM','3:01 PM',
        '6:00 PM','6:01 PM',
        '9:00 PM','9:01 PM',
        '12:00 PM']

        def getAddr(fulladdress):
            if 'OF' in fulladdress:
                pos = str(fulladdress).find('OF')+3
            elif 'BLOCK' in fulladdress:
                pos = str(fulladdress).find('BLOCK')+6
            else:
                return fulladdress
            fulladdress = fulladdress[pos:]
            return fulladdress

        response = urllib.urlopen(url)
        data = json.loads(response.read())
        content = data.get('crimes',False)
        contentList = list(content)
        for single in contentList:
            add =  single.get('address',False)
            if '&' in add:
                twoStr = single.get('address',False)
                twoStreet = twoStr.split('&')

                if getAddr(twoStreet[0]) in dangerous_streets:
                    dangerous_streets[getAddr(twoStreet[0])] = dangerous_streets[getAddr(twoStreet[0])] + 1
                else:
                    dangerous_streets[getAddr(twoStreet[0])] =  1
                
                if getAddr(twoStreet[1]) in dangerous_streets:
                    dangerous_streets[getAddr(twoStreet[1])] = dangerous_streets[getAddr(twoStreet[1])] + 1
                else:
                    dangerous_streets[getAddr(twoStreet[1])] = 1
            else:
                if getAddr(add) in dangerous_streets:
                    dangerous_streets[getAddr(add)] = dangerous_streets[getAddr(add)] + 1
                else:
                    dangerous_streets[getAddr(add)] = 1

            crime_type_count[single.get('type',False)] = crime_type_count.get(single.get('type', False),False) + 1; #need accumunate later
            time = datetime.datetime.strptime(single.get('date',False), '%m/%d/%y %I:%S %p')

            time = time.strftime('%I:%S %p')#prepare to accuminate
            checktime = datetime.datetime.strptime(time, '%I:%S %p')
            if datetime.datetime.strptime(checkTimeList[0],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[1],'%I:%S %p'):
                event_time_count['12:01am-3am'] = event_time_count.get('12:01am-3am', False) + 1
            elif datetime.datetime.strptime(checkTimeList[2],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[3],'%I:%S %p'):
                event_time_count['3:01am-6am'] = event_time_count.get('3:01am-6am', False) + 1
            elif datetime.datetime.strptime(checkTimeList[4],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[5],'%I:%S %p'):
                event_time_count['6:01am-9am'] = event_time_count.get('6:01am-9am', False) + 1
            elif datetime.datetime.strptime(checkTimeList[6],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[7],'%I:%S %p'):
                event_time_count['9:01am-12noon'] = event_time_count.get('9:01am-12noon', False) + 1   
            elif datetime.datetime.strptime(checkTimeList[8],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[9],'%I:%S %p'):
                event_time_count['12:01pm-3pm'] = event_time_count.get('12:01pm-3pm', False) + 1 
            elif datetime.datetime.strptime(checkTimeList[10],'%I:%S %p') <= checktime <= datetime.datetime.strptime(checkTimeList[11],'%I:%S %p'):
                event_time_count['6:01pm-9pm'] = event_time_count.get('6:01pm-9pm', False) + 1
            else:
                event_time_count['9:01pm-12midnight'] = event_time_count.get('9:01pm-12midnight', False) + 1


        if len(dangerous_streets) <= 3:
            the_most_dangerous_streets = dangerous_streets.keys
        else:
            dangerous_streets= sorted(dangerous_streets.iteritems(), key=lambda d:d[1], reverse = True)
            the_most_dangerous_streets = set([])
            for st in dangerous_streets[:3]:
                the_most_dangerous_streets.add(str(st[0]))
        total_crime = str(len(content))

        result = {
            "total_crime" : str(total_crime),
            "the_most_dangerous_streets" : list(the_most_dangerous_streets),
            "crime_type_count" : crime_type_count,
            "event_time_count" : event_time_count
        }
        data1 = json.dumps(result,sort_keys=False)
        print '\n'
        yield data1
        #print '\n'
application = Application([HelloWorldService],
    tns='spyne.examples.hello',
    in_protocol=HttpRpc(),
    out_protocol=JsonDocument()
)
if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()
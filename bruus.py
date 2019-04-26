import os
import time
import datetime
import tushare as ts
import pandas as pd


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# %H %M %S 分别表示时分秒
# >>> a='2019-04-24 14:12:11'
# >>> time1=time.strptime(a, '%Y-%m-%d %H:%M:%S')
# >>> b='14:12:10'
# >>> time2=time.strptime(b, '%H:%M:%S')
# >>> time1 > time2
# >>> True
# 时间转化成秒/秒转化为时间
# >>> datetime.datetime.fromtimestamp(time.mktime(time2))

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# TuShare Pro框架初始化
ts.set_token('Your Token')
pro = ts.pro_api()


# 参数：1：数据 2：开始时间  3：结束时间 4：权重
# 返回：加权后的期望价格
# 数据结构：字典{"time":1997-01-01 ：{ "price":0.01, "volume":1}}
def basePrice(data, start_time, end_time, weight):
    k_start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    k_end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # 转化成秒
    k_start_time_s = time.mktime(k_start_time)
    k_end_time_s = time.mktime(k_end_time)

    total_volume = 0 # 成交总数量
    total_amount = 0 # 成交总价值
    average_price = 0
    for key_time in data:
        # print(data[key_time])
        k_key_time = time.strptime(key_time, '%Y-%m-%d %H:%M:%S')
        k_key_time_s = time.mktime(k_key_time)
        price_volume = data[key_time]
        if((k_key_time_s >= k_start_time_s) and (k_key_time_s <= k_end_time_s)):
            total_volume += price_volume['volume']
            total_amount += price_volume['volume'] * price_volume['price']
    average_price = total_amount/total_volume

    # print(average_price)
    return average_price*weight

# 参数：1: 数据 2：开始时间  3：结束时间 4：基本步长 5：Base Price
# 返回：毛刺数据，结构为time+price的list, 里面是tuple
# 数据结构：字典{"time":"price"}
def burrs(data, start_time, end_time, base_step, base_price):
    k_start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    k_end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # 转化成秒
    k_start_time_s = time.mktime(k_start_time)
    k_end_time_s = time.mktime(k_end_time)


    high_pirce_sets = {} #定义一个字典
    for key_time in data.keys():
        k_key_time = time.strptime(key_time, '%Y-%m-%d %H:%M:%S')
        k_key_time_s = time.mktime(k_key_time)
        if((k_key_time_s < k_start_time_s) or (k_key_time_s > k_end_time_s)):
            continue

        # print("价格：", data[key_time], base_price)
        if data[key_time] > base_price:
            print("符合：", key_time)
            high_pirce_sets.update({key_time: data[key_time]})

    sorted_data = sorted(high_pirce_sets.items())
    if(len(sorted_data) <= 0):
        return {}, 0

    burrs_num = 1
    # print("first time", sorted_data[0][0])
    first_burrs_time = time.mktime(time.strptime(sorted_data[0][0], '%Y-%m-%d %H:%M:%S'))
    current_burrs_time = first_burrs_time
    for item in sorted_data:
        if( time.mktime(time.strptime(item[0], '%Y-%m-%d %H:%M:%S')) - current_burrs_time < base_step ):
            current_burrs_time = time.mktime(time.strptime(item[0], '%Y-%m-%d %H:%M:%S'))
        else:
            burrs_num = burrs_num + 1
            current_burrs_time = time.mktime(time.strptime(item[0], '%Y-%m-%d %H:%M:%S'))
    # for key_time in sorted(high_pirce_sets.keys()):
    #    print("======", key_time)

    # print(high_pirce_sets)
    return sorted_data, burrs_num


# 当日历史实时数据 - 每只票20秒获取一次 get_today_ticks('000001')
# today_data = ts.get_today_ticks('000001')


# 参数1. 遍历股票 ['000001', '000002', '000003', '000004', '000005', '000006']
# 参数2. 遍历日期 ['2019-04-22', '2019-04-23', '2019-04-24', '2019-04-25', '2019-04-26']
# 参数3. 数据保存路径，csv数据命名由tick-code和date组成。
# Save Data

def get_ticks_days_burrs(tick_codes, date_array, weight, data_save_path):
    if not os.path.exists(data_save_path):
            os.makedirs(data_save_path)
    for tick_code in tick_codes:
        for date in date_array:
            data_sava_path = os.path.join(data_save_path, tick_code + '_' + date + ".csv")
            print(data_sava_path)
            # 数据已经不存在就下载保存，不重复下载
            if not os.path.exists(data_sava_path):
                his_real_timep_data = ts.get_tick_data(tick_code, date=date, src='tt')
                his_real_timep_data.to_csv(data_sava_path, header=None)
            names = ['time', 'price', 'change', 'volume', 'amount', 'type']
            data = pd.read_csv(data_sava_path, names=names)
            # 加权基准价格的输入字典数据
            base_price_data = {}
            for index, row in data.iterrows():
                base_price_data.update({date + ' ' + row['time']: {'price' : row['price'], 'volume' : row['volume']}})
            base_weight_price = basePrice(base_price_data, date + ' 09:30:00', date + ' 11:30:00', weight)

            base_price_data.clear()

            # 毛刺数据
            bruus_data = {}
            for index, row in data.iterrows():
                bruus_data.update({date + ' ' + row['time']: row['price']})
            print(date + ' 13:00:00')
            a,b = burrs(bruus_data, date + ' 09:30:00', date + ' 11:30:00', 60, base_weight_price)
            print(tick_code + ":" + date + " :", base_weight_price)
            print(b)

            bruus_data.clear()


tick_code_list = ['000001']
data_array = ['2019-04-22', '2019-04-23', '2019-04-24', '2019-04-25']

get_ticks_days_burrs(tick_code_list, data_array, 1.01, "./data")

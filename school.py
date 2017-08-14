# -*- coding: utf-8 -*-
"""
__author__ = 'lwl224'
__mtime__ = '2017/8/3'
"""
import sys
from pandas import DataFrame, read_csv, read_excel, to_datetime, merge, ExcelWriter, pivot_table, np
from time import clock
import numpy as np

reload(sys)
sys.setdefaultencoding('utf8')




def ltecell():
    pass



    

def looding():
    start = clock()
    data1 = '0814'
    data2 = '0811'
    dflte = read_csv(ur'4G指标%s.csv' % data1, skiprows=[0], header=None, encoding="gbk")
    dfwcdma = read_csv(ur'%swcdma.csv' % data1, skiprows=[0], header=None, encoding="gbk")
    dfgsm = read_excel(ur'%sgsm.xls' % data1, skiprows=[0], header=None, sheetname='Sheet')
    dfnamelist = read_excel(ur'校园清单0802.xlsx', skiprows=[0], header=None)
    end = clock()
    print str((end - start) / 60) + 'mins'
    print u'完成读取'
    start = clock()
    dflte[11] = dflte[7].astype(float) + dflte[8].astype(float)
    dflte[11] = dflte[11] / (1024 * 1024)
    dflte[12] = dflte[2] * 256 + dflte[3]

    dfgsm[5] = dfgsm[5] / (1024 * 1024 * 1024)
    dfwcdma[19] = dfwcdma[10] + dfwcdma[11]
    dfwcdma[19] = dfwcdma[19] / (1024 * 1024 * 1024)
    varssnamelte1 = dfnamelist[dfnamelist[6].str.contains(u'LTE')]
    varssnamelte = varssnamelte1.ix[:, [8]]
    varssnamewcdma = dfnamelist[dfnamelist[6].str.contains(u'WCDMA')]
    varssnamegsm = dfnamelist[dfnamelist[6].str.contains(u'GSM')]
    varssnamewcdmagsm1 = varssnamewcdma.append(varssnamegsm, ignore_index=True)
    varssnamewcdmagsm = varssnamewcdmagsm1.ix[:, [7, 8]]
    varssnamewcdmagsm.columns = ['LAC', 'CI']
    varssnamelte.columns = ['cellid']
    varssnamelte = varssnamelte.drop_duplicates(['cellid'])
    dflte.columns = ['city', 'changjia', 'ENODB ID', 'id', 'cellid1', 'data', 'time', 'shang', 'xia', 'PRB', 'rrc',
                     'ltetraffic', 'cellid']
    dfgsm.columns = ['time', 'nodbname', 'LAC', 'CI', 'huawu', 'gsmtraffic', 'city', 'PRB']
    dfwcdma.columns = ['0', 'city', '2', '3', 'LAC', 'CI', '6', '7', '8', 'huawu', '10', '11', '12', '13', '14', '15', '16',
                       '17', 'PRB', 'wcdmatraffic']
    dflteschool = merge(dflte, varssnamelte, how='inner', on='cellid')
    dfgsmschool = merge(dfgsm, varssnamewcdmagsm, how='inner', on=['LAC', 'CI'])
    dfwcdmaschool = merge(dfwcdma, varssnamewcdmagsm, how='inner', on=['LAC', 'CI'])
    # dflteschool_zero = dflteschool[dflteschool['ltetraffic']  == 0]
    dfgsmschool_zero = dfgsmschool[dfgsmschool['huawu']  == 0]
    dfwcdmaschool_zero = dfwcdmaschool[dfwcdmaschool['huawu']  == 0 ]
    dfwcdmaschool_zero = dfwcdmaschool_zero [dfwcdmaschool_zero ['wcdmatraffic']  == 0 ]

    pivot_tablelte1_zero = pivot_table(dflte, index=[ 'cellid1','city'], values=['ltetraffic'], aggfunc=[np.sum, len])
    dflteschool_zero =pivot_tablelte1_zero[pivot_tablelte1_zero.icol(0)==0]
    dflteschool_zero =dflteschool_zero[dflteschool_zero.icol(1)==24]
    dflteschool_zero['ltetraffic'] = dflteschool_zero.icol(1)/24
    dflteschool_zero.index.names=['key1','city']
    # print dflteschool_zero['ltetraffic'].sum(level='city')
    pivot_tablelte = pivot_table(dflte, index=['city'], values=['ltetraffic', 'PRB'], aggfunc=[np.sum, np.mean, len])
    pivot_tablewcdma = pivot_table(dfwcdma, index=['city'], values=['wcdmatraffic', 'PRB'],
                                   aggfunc=[np.sum, np.mean, len])
    pivot_tablegsm = pivot_table(dfgsm, index=['city'], values=['gsmtraffic', 'PRB'], aggfunc=[np.sum, np.mean, len])
    pivot_tableschoollte = pivot_table(dflteschool, index=['city'], values=['ltetraffic', 'PRB'],
                                       aggfunc=[np.sum, np.mean, len])
    pivot_tableschoolwcdma = pivot_table(dfwcdmaschool, index=['city'], values=['wcdmatraffic', 'PRB'],
                                         aggfunc=[np.sum, np.mean, len])
    pivot_tableschoolgsm = pivot_table(dfgsmschool, index=['city'], values=['gsmtraffic', 'PRB'],
                                       aggfunc=[np.sum, np.mean, len])
    tempcell = DataFrame([pivot_tablelte.iloc[:, 1], pivot_tablewcdma.iloc[:, 1], pivot_tablegsm.iloc[:, 1]])
    tempcell = DataFrame(tempcell.T,
                         index=[u'南宁', u'柳州', u'桂林', u'崇左', u'梧州', u'贵港', u'百色', u'北海', u'玉林', u'钦州', u'贺州', u'来宾',
                                u'防城港', u'河池'])

    tempcell[4] = tempcell.icol(0) + tempcell.icol(1) + tempcell.icol(2)
    tempschool = DataFrame(
        [pivot_tableschoollte.iloc[:, 1], pivot_tableschoolwcdma.iloc[:, 1], pivot_tableschoolgsm.iloc[:, 1]], )
    tempschool = DataFrame(tempschool.T,
                           index=[u'南宁', u'柳州', u'桂林', u'崇左', u'梧州', u'贵港', u'百色', u'北海', u'玉林', u'钦州', u'贺州', u'来宾',
                                  u'防城港', u'河池'])
    tempschool[4] = tempschool.icol(0) + tempschool.icol(1) + tempschool.icol(2)
    tempschoolprb = DataFrame(
        [pivot_tableschoollte.iloc[:, 2] / (0.9 * 0.6 * 0.5 * 0.01), pivot_tableschoolwcdma.iloc[:, 2],
         pivot_tableschoolgsm.iloc[:, 2]], )
    tempschoolprb = DataFrame(tempschoolprb.T,
                              index=[u'南宁', u'柳州', u'桂林', u'崇左', u'梧州', u'贵港', u'百色', u'北海', u'玉林', u'钦州', u'贺州', u'来宾',
                                     u'防城港', u'河池'])
    tempschool_zero = DataFrame(dflteschool_zero['ltetraffic'].sum(level='city'),
                           index=[u'南宁', u'柳州', u'桂林', u'崇左', u'梧州', u'贵港', u'百色', u'北海', u'玉林', u'钦州', u'贺州', u'来宾',
                                  u'防城港', u'河池'])
    end = clock()
    print str((end - start) / 60) + 'mins'
    print u'完成统计'
    start = clock()
    with ExcelWriter('result_alltest%s.xlsx' % data1) as writer:
        # varssnamelte.to_excel(writer, sheet_name=u'lte列表', encoding="gbk")
        # dflteschool.to_excel(writer, sheet_name=u'lte学校列表',encoding="gbk")
        # Statistical_celllte.to_excel(writer, sheet_name=u'lte按地市流量统计', encoding="gbk")
        # Statistical_cell_schoollte.to_excel(writer, sheet_name=u'lte校园按地市流量统计', encoding="gbk")
        tempcell.to_excel(writer, sheet_name=u'按地市流量汇总', encoding="gbk")
        tempschool.to_excel(writer, sheet_name=u'按校园地市流量汇总', encoding="gbk")
        tempschoolprb.to_excel(writer, sheet_name=u'按校园地市资源利用率汇总', encoding="gbk")
        tempschool_zero.to_excel(writer, sheet_name=u'零话务小区汇总', encoding="gbk")
        pivot_tablelte.to_excel(writer, sheet_name=u'lte按地市透视图', encoding="gbk")
        pivot_tableschoollte.to_excel(writer, sheet_name=u'lte校园按地市透视图', encoding="gbk")
        dflteschool.to_excel(writer, sheet_name=u'lte校园按地市清单', encoding="gbk")
        # Statistical_cellgsm.to_excel(writer, sheet_name=u'gsm按地市流量统计', encoding="gbk")
        # Statistical_cell_schoolgsm.to_excel(writer, sheet_name=u'gsm校园按地市流量统计', encoding="gbk")
        pivot_tablegsm.to_excel(writer, sheet_name=u'gsm按地市透视图', encoding="gbk")
        pivot_tableschoolgsm.to_excel(writer, sheet_name=u'gsm校园按地市透视图', encoding="gbk")


        # Statistical_cellwcdma.to_excel(writer, sheet_name=u'wcdma按地市流量统计', encoding="gbk")
        # Statistical_cell_schoolwcdma.to_excel(writer, sheet_name=u'wcdma校园按地市流量统计', encoding="gbk")
        pivot_tablewcdma.to_excel(writer, sheet_name=u'wcdma按地市透视图', encoding="gbk")
        pivot_tableschoolwcdma.to_excel(writer, sheet_name=u'wcdma校园按地市透视图', encoding="gbk")
        dflteschool_zero.to_excel(writer, sheet_name=u'lte校园零话务小区清单', encoding="gbk")
        dfwcdmaschool_zero.to_excel(writer, sheet_name=u'wcdma校园零话务小区清单', encoding="gbk")
        dfgsmschool_zero.to_excel(writer, sheet_name=u'gsm校园零话务小区清单', encoding="gbk")
        print str((end - start) / 60) + 'mins'
        print u'完成输出文档'


looding()

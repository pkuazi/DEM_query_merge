import fiona
import pgsql
from shapely.geometry import mapping, Polygon

pg_src = pgsql.Pgsql("10.0.81.35", "2345","postgres", "", "gscloud_metadata")

def region_search_dem(min_lat, max_lat, min_long, max_long, dst_shp):
    data_sql = '''SELECT id, dataid, name, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat,the_geom FROM public.metadata_dem_gdem where lt_long>%s and rb_long<%s and lt_lat<%s and rb_lat>%s ORDER BY row DESC;'''%(min_long,max_long,max_lat,min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    #output bounding box into shp
    dataid_list=[]
    
     # schema is a dictory
    schema={'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'path':'int','row':'int'} }
    #  use fiona.open
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(num):
            record = dem_data[i]
            bbox = dem_data[i][9]
            dataid = dem_data[i][1]
            if dataid.startswith('ASTGTM2'):
                dataid_list.append(dataid)
                minx,maxy,maxx,miny = dem_data[i][5],dem_data[i][6],dem_data[i][7],dem_data[i][8]
                poly=Polygon([[minx,maxy],[maxx,maxy],[maxx,miny],[minx,miny],[minx,maxy]])
                element = {'geometry':mapping(poly), 'properties': {'id': i, 'dataid': dataid,'path':dem_data[i][3],'row':dem_data[i][4]}}
                layer.write(element)     
    return  dataid_list  
def region_search_srtm(min_lat, max_lat, min_long, max_long, dst_shp):
    data_sql = '''SELECT id, dataid, "path", "row",  lt_long, lt_lat,  rb_long, rb_lat FROM public.metadata_dem_srtm where lt_long>%s and rb_long<%s and lt_lat<%s and rb_lat>%s ORDER BY row DESC;'''%(min_long,max_long,max_lat,min_lat)
    dem_data = pg_src.getAll(data_sql)
    num = len(dem_data)
    
    #output bounding box into shp
    dataid_list=[]
    
     # schema is a dictory
    schema={'geometry': 'Polygon', 'properties': {'id': 'int', 'dataid': 'str', 'path':'int','row':'int'} }
    #  use fiona.open
    with fiona.open(dst_shp, mode='w', driver='ESRI Shapefile', schema=schema, crs='EPSG:4326', encoding='utf-8') as layer:
        for i in range(num):
            record = dem_data[i]

            dataid = dem_data[i][1]
            if dataid.startswith('srtm'):
                dataid_list.append(dataid)
                minx,maxy,maxx,miny = dem_data[i][4],dem_data[i][5],dem_data[i][6],dem_data[i][7]
                poly=Polygon([[minx,maxy],[maxx,maxy],[maxx,miny],[minx,miny],[minx,maxy]])
                element = {'geometry':mapping(poly), 'properties': {'id': i, 'dataid': dataid,'path':dem_data[i][3],'row':dem_data[i][4]}}
                layer.write(element)     
    return  dataid_list  
def merge_spec_dem(dataid_list, row_min, row_max, path_min,path_max, dstfile_path):
    #     dem_path='/mnt/gscloud/DEM/gdem30v2/gdem30v2'
    dem_path = '/mnt/gscloud/DEM/unzip0/GDTM/'
    dataname='/mnt/gscloud/DEM/unzip/GDTM/ASTGTM2_N34W087/ASTGTM2_N34W087_dem.tif'
    num = len(dataid_list)
    srcfiles1 = ''
    for i in range(num):
        dataid = dataid_list[i]
        print(dataid)
        row = int(dataid[9:11])
        path = int(dataid[12:15])
        if path>=path_min and path <=path_max:
            if row>=row_min and row <=row_max:
                data_path = os.path.join(dem_path,dataid_list[i])
                srcfiles1 = srcfiles1+os.path.join(data_path,str(dataid_list[i])+'_dem.tif')
                srcfiles1 = srcfiles1+' '
                if not os.path.exists(data_path):
                    print(data_path)
                    unzip_path=os.path.join(dem_path,dataid)
                    mkdir_cmd = 'mkdir %s'%(unzip_path)
                    os.system(mkdir_cmd)
                    unzip_cmd = 'unzip %s -d %s'%(os.path.join("/mnt/gscloud/DEM/gdem30v2/gdem30v2",dataid+'.zip'),unzip_path)
                    os.system(unzip_cmd)

    merge_cmd1 = 'gdalwarp %s %s'%(srcfiles1, dstfile_path)
    os.system(merge_cmd1) 
    
def merge_spec_srtm(dataid_list, row_min, row_max, path_min,path_max, dstfile_path):
    #     dem_path='/mnt/gscloud/DEM/gdem30v2/gdem30v2'
    dem_path = '/mnt/gscloud/DEM/unzip0/GDTM/'
    dataname='/mnt/gscloud/DEM/unzip/GDTM/ASTGTM2_N34W087/ASTGTM2_N34W087_dem.tif'
    num = len(dataid_list)
    srcfiles1 = ''
    for i in range(num):
        dataid = dataid_list[i]
        print(dataid)
        row = int(dataid[9:11])
        path = int(dataid[12:15])
        if path>=path_min and path <=path_max:
            if row>=row_min and row <=row_max:
                data_path = os.path.join(dem_path,dataid_list[i])
                srcfiles1 = srcfiles1+os.path.join(data_path,str(dataid_list[i])+'_dem.tif')
                srcfiles1 = srcfiles1+' '
                if not os.path.exists(data_path):
                    print(data_path)
                    unzip_path=os.path.join(dem_path,dataid)
                    mkdir_cmd = 'mkdir %s'%(unzip_path)
                    os.system(mkdir_cmd)
                    unzip_cmd = 'unzip %s -d %s'%(os.path.join("/mnt/gscloud/DEM/gdem30v2/gdem30v2",dataid+'.zip'),unzip_path)
                    os.system(unzip_cmd)

    merge_cmd1 = 'gdalwarp %s %s'%(srcfiles1, dstfile_path)
    os.system(merge_cmd1) 

import os,gdal
def getbox_from_image(file):
    ds = gdal.Open(file)
    rows, cols=ds.RasterYSize, ds.RasterXSize
    geot=ds.GetGeoTransform()
    minx=geot[0]
    maxy=geot[3]
    maxx=minx+cols*geot[1]
    miny =maxy+geot[5]*rows
    return minx,maxy,maxx,miny

path=os.getcwd()

import pandas as pd
def bbox_pd_sort(path):
    all_files = [f for f in os.listdir(path)]
    
    file_list=[]
    minx_list=[]
    maxy_list=[]
    maxx_list=[]
    miny_list=[]
    for i in range(len(all_files)):
        if all_files[i].endswith('.tif'):
            bbox = getbox(os.path.join(path,all_files[i]))
            file_list.append(all_files[i])
            minx_list.append(bbox[0])
            maxy_list.append(bbox[1])
            maxx_list.append(bbox[2])
            miny_list.append(bbox[3])
    bb = {}
    bb["filename"]=file_list
    bb["minx"]=minx_list
    bb["maxy"]=maxy_list
    bb["maxx"]=maxx_list
    bb["miny"]=miny_list
    
    df=pd.DataFrame(bb)
    yd=df.sort_values(by='maxy',ascending=False)
    yd.to_csv('yd.csv')
    xs=df.sort_values(by='minx', ascending=True)
    xs.to_csv('xs.csv')

if __name__ == "__main__":
#     #for japan
#     min_lat=25
#     max_lat=65
#     min_long=125
#     max_long=165
#     shp_file="/tmp/jpan_bbox.shp"
#     jp_data = region_search_dem(min_lat, max_lat, min_long, max_long,shp_file)
# # #     merge_all_dem(data)
# #     
# #     region1_row_min=46
# #     region1_row_max=63
# #     region1_path_min=126
# #     region1_path_max=143
# #     
# #     dstfiles1 = '/tmp/dstfile1.tif'
# #     merge_spec_dem(jp_data, region1_row_min, region1_row_max, region1_path_min,region1_path_max, dstfiles1)
# #     
#     region2_row_min=46
#     region2_row_max=63
#     region2_path_min=144
#     region2_path_max=163
#     dstfiles2 = '/mnt/data/jp2.tif'
#     merge_spec_dem(jp_data, region2_row_min, region2_row_max, region2_path_min,region2_path_max, dstfiles2)
#      
#     region3_row_min=26
#     region3_row_max=44
#     region3_path_min=126
#     region3_path_max=150
#     dstfiles3 = '/mnt/data/jp3.tif'
#     merge_spec_dem(jp_data, region3_row_min, region3_row_max, region3_path_min,region3_path_max, dstfiles3)
    
#     # for arctic
#     min_lat=59
#     max_lat=82.5
#     min_long=4
#     max_long=103.5
#     shp_file='/tmp/arctic_bbox.shp'
#     arc_data = region_search_dem(min_lat, max_lat, min_long, max_long,shp_file)
#     
#     row_min=60
#     row_max=81
#     r1_path_min=5
#     r1_paht_max=29
#     r2_path_min=30
#     r2_path_max=54
#     r3_path_min=55
#     r3_path_max=79
#     r4_path_min=80
#     r4_path_max=101
#     merge_spec_dem(arc_data, row_min, row_max, r1_path_min,r1_paht_max, '/root/arc1.tif')
#     merge_spec_dem(arc_data, row_min, row_max, r2_path_min,r2_paht_max, '/root/arc2.tif')
#     merge_spec_dem(arc_data, row_min, row_max, r3_path_min,r3_paht_max, '/root/arc3.tif')
#     merge_spec_dem(arc_data, row_min, row_max, r4_path_min,r4_paht_max, '/root/arc4.tif')
    
    
    # for xibei
    min_lat=-65
    max_lat=85
    min_long=-180
    max_long=-30
    shp_file='/tmp/meizhou_srtm_bbox.shp'
    data = region_search_srtm(min_lat, max_lat, min_long, max_long,shp_file)
    
    
    
    
    



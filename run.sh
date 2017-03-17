#!/bin/sh

export JAVA_HOME=/usr/bin/hadoop/software/java
export HBASE_HOME=/usr/bin/hadoop/software/hbase
export HADOOP_HOME=/usr/bin/hadoop/software/hadoop
export PATH=$JAVA_HOME/bin:$HBASE_HOME/bin:$HADOOP_HOME/bin:$PATH
export LANG="en_US.UTF-8"

LOCATION_DIR=/home/hdp-skyeye/data/skyeye_geo/locations
OUTPUT_DIR=/home/hdp-skyeye/proj/hdp-skyeye-arp/skyeye_geo
STREAMING_JAR_PATH=/home/work/software/hadoop/contrib/streaming/hadoop-streaming.jar


function log()
{
    echo -e $1
    echo -e $1 >> script.log>&1 
}


function mergeAll()
{
# 根据ip归并所有的地理信息文件，输出每个ip对应的最新时间以及地理信息，这个输出和最新的地理信息合并的初始条件
now=$(date +%Y%m%d)
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=100 \
    -D mapred.job.name="j-maxinmin_mergeAllGeoInfomation" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.compress.map.output=true \
    -D mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $LOCATION_DIR/*/part-* \
    -output $OUTPUT_DIR/last_ip_geo_info/$now \
    -mapper "mr.py mapper" \
    -reducer "mr.py reducer" \
    -file "mr.py" ;
}


function transferFileFormat()
{
if [ $# -lt 1 ];then
    echo "need 1 param: please specify the geo info date for updating" 
    exit
fi
# 原始的文件格式为ip|geo_info，将该格式转化为`ip \t 20160812 \t geo_info`
hadoop fs -rmr -skipTrash $OUTPUT_DIR/tmp/geo_transfer/$1
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=10 \
    -D mapred.job.name="j-maxinmin_transferGeoFileFormat" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.compress.map.output=true \
    -D mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $LOCATION_DIR/$1/part-* \
    -output $OUTPUT_DIR/tmp/geo_transfer/$1 \
    -mapper "mr.py mapper" \
    -file "mr.py" ;
}


function generateLastGeoInfo()
{
# 生成最新的地理信息。第一个参数为最新的日期，第二个参数为要更新的日期
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=100 \
    -D mapred.job.name="j-maxinmin_updateIpGeoInfo" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.compress.map.output=true \
    -D mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $OUTPUT_DIR/last_ip_geo_info/$1 \
    -input $OUTPUT_DIR/tmp/geo_transfer/$2 \
    -output $OUTPUT_DIR/last_ip_geo_info/$2 \
    -mapper "mr.py origin_mapper" \
    -reducer "mr.py reducer" \
    -file "mr.py" ;
}


function importLastIpGeoToEs()
{
date=$1
# 生成最新的地理信息。第一个参数为最新的日期，第二个参数为要更新的日期
hadoop fs -rmr -skipTrash $OUTPUT_DIR/tmp/es_error_log
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=0 \
    -D mapred.job.name="j-maxinmin_importLastIpGeoToEs" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -input $OUTPUT_DIR/last_ip_geo_info/$date \
    -output $OUTPUT_DIR/tmp/es_error_log \
    -mapper "./python/bin/python geo_to_es.py import_last_ipgeo" \
    -file "geo_to_es.py" \
    -cacheArchive "/home/hdp-skyeye/software/python_cjson.tgz#python" ;
}


function exportGeoDataToLocal()
{
    date=$1;
    localpath=$(pwd)/result/$1
    mkdir $localpath
    echo 'localpath:' $localpath

    for i in {00..02}
    do
        filepath=$OUTPUT_DIR/last_ip_geo_info/$date/part-000$i.gz
        echo 'extract geo info from hdfs:' $filepath
        hadoop fs -text $filepath | awk '{print $3}' > $localpath/part$i.txt
    done

    cd ./result
    tar -czvf $1.tar.gz ./$1
    cd ../
}


DEBUG=1
if [ $DEBUG == 0 ]; then
    log "In No DEBUG mode"
    partFile=part-*
else
    log "In DEBUG mode"
    #partFile=part-000[5-9][0-9].gz
    partFile=part-r-*.gz
fi


function requestGeoDetail()
{
# 向高德api发送请求，根据经纬度获取对应的地理信息
    #-outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
    #-jobconf suffix.multiple.outputformat.filesuffix="part,error"  \  
    #-jobconf suffix.multiple.outputformat.separator="#" \  
date=$1
hadoop fs -rmr -skipTrash $OUTPUT_DIR/geo_detail/$date/$2
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=100 \
    -D mapred.job.name="j-maxinmin_requestGeoDetail" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $OUTPUT_DIR/last_ip_geo_info/$date/$partFile \
    -output $OUTPUT_DIR/geo_detail/$date/$2 \
    -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
    -jobconf suffix.multiple.outputformat.filesuffix=part,warn,err \
    -jobconf suffix.multiple.outputformat.separator="#" \
    -mapper "./python/bin/python request_geo_mr.py mapper" \
    -reducer "./python/bin/python request_geo_mr.py reducer" \
    -file "geo_utils.py" \
    -file "request_geo_mr.py" \
    -cacheArchive "/home/hdp-skyeye/software/python_cjson.tgz#python"
}


function requestGeoSmallTest()
{
    #-reducer "./python/bin/python request_geo_mr.py reducer" \
hadoop fs -rmr -skipTrash $OUTPUT_DIR/tmp/small_test_output
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=10 \
    -D mapred.job.name="j-maxinmin_requestGeoSmallTest" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $OUTPUT_DIR/tmp/small_test_input/part.txt \
    -output $OUTPUT_DIR/tmp/small_test_output \
    -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
    -jobconf suffix.multiple.outputformat.filesuffix=part,warn,err \
    -jobconf suffix.multiple.outputformat.separator="#" \
    -mapper "./python/bin/python request_geo_mr.py mapper" \
    -reducer "./python/bin/python request_geo_mr.py reducer" \
    -file "geo_utils.py" \
    -file "request_geo_mr.py" \
    -cacheArchive "/home/hdp-skyeye/software/python_cjson.tgz#python"
}


function exportGeoDetaiToEs()
{
# 将获取的地理信息导入到ES

#-D stream.map.output.field.separator="|" \
#-D stream.num.map.output.key.fields=1 \
#-input $OUTPUT_DIR/geo_detail/$date/$partFile \

date=$1
# 创建最新的索引
#curl -XPUT "http://10.138.77.205:9200/create_index_action/skyeye-ip-geo${1}/"

hadoop fs -rmr -skipTrash $OUTPUT_DIR/tmp/export_geo_es_log1
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=100 \
    -D mapred.job.name="j-maxinmin_exportGeoDetaiToEs" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.compress.map.output=true \
    -D mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $OUTPUT_DIR/geo_detail/$date/2/$partFile \
    -output $OUTPUT_DIR/tmp/export_geo_es_log1 \
    -mapper "./python/bin/python geo_to_es.py mapper" \
    -reducer  "./python/bin/python geo_to_es.py import_geo_detail $date" \
    -file "geo_to_es.py" \
    -cacheArchive "/home/hdp-skyeye/software/python_cjson.tgz#python"
}


function runAll()
{
    echo "该脚本要执行两天左右，如果查看脚本进度以及步骤信息，请查看本目录的script.log文件"
    log "正在执行runAll命令：将最新的location日期${2}增量合并到${1} ..."
    log "原始目录：$OUTPUT_DIR/last_ip_geo_info/${1}"
    log "最新目录：$LOCATION_DIR/${2}"
    log "警告：请确保以上目录存在于hdfs上\n"

    log "1. 正在转换最新目录文件，生成临时文件，以统一文件格式\n"
    transferFileFormat $2;

    log "2. 正在与原始目录合并，最新的ip location信息存在于目录：$OUTPUT_DIR/last_ip_geo_info/${2}\n"
    generateLastGeoInfo $1 $2;

    log "3. 通过http向高德API爬取经纬度的具体信息\n"
    requestGeoDetail $2

    log "4. 将从高德获取的数据插入到ES中\n"
    exportGeoDetaiToEs $2

    log "Success!"
}


function requestTest()
{
# 向高德api发送请求，根据经纬度获取对应的地理信息
date=$1;
hadoop fs -rmr -skipTrash $OUTPUT_DIR/geo_detail/test1
hadoop jar $STREAMING_JAR_PATH \
    -D mapred.reduce.tasks=100 \
    -D mapred.job.name="j-maxinmin_requestGeoTest" \
    -D mapred.job.priority="VERY_HIGH" \
    -D mapred.success.file.status=true \
    -D mapred.reduce.tasks.speculative.execution=false \
    -D mapred.compress.map.output=true \
    -D mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -input $OUTPUT_DIR/last_ip_geo_info/$date/part-* \
    -output $OUTPUT_DIR/geo_detail/test1 \
    -mapper "request_geo_mr.py mapper" \
    -reducer "request_geo_mr.py reducer" \
    -file "geo_utils.py" \
    -file "request_geo_mr.py" ;
}


function test()
{
    echo "test"
    exportGeoDetaiToEs 20161219
    #requestGeoDetail 20161219 2
    #requestTest 20161219
    #importLastIpGeoToEs 20161219
    #requestGeoSmallTest
}


# main
case $1 in
"runAll")
    if [ $# -lt 3 ];then
        echo "need 3 params. Usage: ./run.sh runAll 20160812 20161123"
        exit
    fi
    runAll $2 $3
    ;;
"mergeAll") mergeAll
    ;;
"formatGeoFile")
    if [ $# -lt 2 ];then
        echo "need 2 params. Usage: ./run.sh formatGeoFile 20161129 "
        exit
    fi
    transferFileFormat $2
    ;;
"generateLastGeoInfo")
    if [ $# -lt 3 ];then
        echo "need 3 params. Usage: ./run.sh generateLastGeoInfo 20160812 20161123"
        exit
    fi
    generateLastGeoInfo $3 $2;
    ;;
"exportToLocal")
    exportGeoDataToLocal $2;
    ;;
"test")
    test 
    ;;
*)
    echo "Usage: ./run.sh [runAll | mergeAll | formatGeoFile]"
    ;;
esac


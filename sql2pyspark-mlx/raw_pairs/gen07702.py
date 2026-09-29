from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").desc(), "id")
w2 = Window.orderBy("pressure")
w3 = Window.partitionBy(F.month("reading_ts"))
w4 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy("humidity", "id").rowsBetween(-6, -1)

result = (
    df_device_readings
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("pressure_rank", F.rank().over(w2))
    .withColumn("corr_status_bits_pressure", F.corr("status_bits", "pressure").over(w3))
    .withColumn("humidity_sum_2h", F.sum("humidity").over(w4))
    .withColumn("pressure_avg_6_prior", F.avg("pressure").over(w5))
)

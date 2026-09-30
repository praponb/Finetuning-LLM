from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("temp_c").isNull())
    .withColumn("temp_c_times_status_bits", (F.col("temp_c") * F.col("status_bits")))
    .withColumn("temp_c_times_humidity", (F.col("temp_c") * F.col("humidity")))
)

result = (
    cleaned
    .withColumn("status_bits_rank", F.rank().over(w))
    .withColumn("temp_c_times_status_bits_sum_15m", F.sum("temp_c_times_status_bits").over(w2))
    .withColumn("temp_c_times_humidity_sum_1h", F.sum("temp_c_times_humidity").over(w3))
)

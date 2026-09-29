from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.orderBy("humidity", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy(F.col("status_bits").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

result = (
    df_device_readings
    .withColumn("pressure_sum_2h", F.sum("pressure").over(w))
    .withColumn("temp_c_avg_2_prior", F.avg("temp_c").over(w_recent))
    .withColumn("cumulative_temp_c", F.sum("temp_c").over(w3))
    .withColumn("status_bits_bucket", F.ntile(10).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w5))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy("status_bits", "id")
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("dv_03_max_device_code_avg_humidity").desc())

metrics = (
    df_device_readings
    .withColumn("device_code_avg_humidity", F.avg("humidity").over(w))
    .withColumn("humidity_avg_1h", F.avg("humidity").over(w2))
    .withColumn("next_pressure", F.lead("pressure", 2).over(w_running))
    .withColumn("running_temp_c", F.sum("temp_c").over(w4))
)

result = (
    metrics
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.max(F.when(F.col("device_code") == "DV-03", F.col("device_code_avg_humidity"))).alias("dv_03_max_device_code_avg_humidity"),
        F.max("reading_ts").alias("last_reading_ts"),
    )
    .withColumn("dv_03_max_device_code_avg_humidity_percent_rank", F.percent_rank().over(w5))
)

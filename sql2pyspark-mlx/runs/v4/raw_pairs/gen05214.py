from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("humidity")).otherwise(0)).alias("dv_03_humidity"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("temp_c")).otherwise(0)).alias("dv_03_temp_c"),
    )
    .orderBy(F.col("dv_03_temp_c").desc())
)

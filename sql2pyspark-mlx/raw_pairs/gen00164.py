from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-02"])
    .agg(F.avg("humidity").alias("total"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-02").alias("dv_02"))
)

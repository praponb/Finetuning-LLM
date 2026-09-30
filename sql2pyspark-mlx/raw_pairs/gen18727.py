from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-03"])
    .agg(F.avg("humidity").alias("amount"))
    .select("firmware", F.col("DV-04").alias("dv_04"), F.col("DV-03").alias("dv_03"))
)

from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("temp_c") <= 28)
    .withColumn("pressure_rounded", F.round("pressure", 2))
)

result = (
    filtered
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04"])
    .agg(F.avg("temp_c").alias("value_sum"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-04").alias("dv_04"))
)

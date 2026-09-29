from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-02"])
    .agg(F.avg("temp_c"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-02").alias("dv_02"))
)

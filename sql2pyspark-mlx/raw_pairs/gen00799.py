from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-04"])
    .agg(F.avg("humidity").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-01_value_sum").alias("dv_01_value_sum"),
        F.col("DV-01_cnt").alias("dv_01_cnt"),
        F.col("DV-04_value_sum").alias("dv_04_value_sum"),
        F.col("DV-04_cnt").alias("dv_04_cnt"),
    )
)

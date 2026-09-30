from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-04"])
    .agg(F.sum("temp_c").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-01_total").alias("dv_01_total"),
        F.col("DV-01_cnt").alias("dv_01_cnt"),
        F.col("DV-04_total").alias("dv_04_total"),
        F.col("DV-04_cnt").alias("dv_04_cnt"),
    )
)

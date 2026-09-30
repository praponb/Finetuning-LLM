from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04"])
    .agg(F.sum("humidity").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-03_amount").alias("dv_03_amount"),
        F.col("DV-03_cnt").alias("dv_03_cnt"),
        F.col("DV-04_amount").alias("dv_04_amount"),
        F.col("DV-04_cnt").alias("dv_04_cnt"),
    )
)

from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-02"])
    .agg(F.sum("temp_c").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-01_amount").alias("dv_01_amount"),
        F.col("DV-01_cnt").alias("dv_01_cnt"),
        F.col("DV-02_amount").alias("dv_02_amount"),
        F.col("DV-02_cnt").alias("dv_02_cnt"),
    )
)

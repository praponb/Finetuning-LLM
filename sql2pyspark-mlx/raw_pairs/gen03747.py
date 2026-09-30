from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-02"])
    .agg(F.avg("status_bits").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-01_amount").alias("dv_01_amount"),
        F.col("DV-01_n").alias("dv_01_n"),
        F.col("DV-02_amount").alias("dv_02_amount"),
        F.col("DV-02_n").alias("dv_02_n"),
    )
)

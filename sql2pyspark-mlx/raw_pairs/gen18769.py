from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-04", "DV-01"])
    .agg(F.avg("status_bits").alias("value_sum"))
    .select(
        "firmware",
        F.col("DV-02").alias("dv_02"),
        F.col("DV-04").alias("dv_04"),
        F.col("DV-01").alias("dv_01"),
    )
)

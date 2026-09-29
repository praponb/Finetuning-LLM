from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-03", "DV-04"])
    .agg(F.max("temp_c").alias("value_sum"))
    .select(
        "firmware",
        F.col("DV-02").alias("dv_02"),
        F.col("DV-03").alias("dv_03"),
        F.col("DV-04").alias("dv_04"),
    )
)

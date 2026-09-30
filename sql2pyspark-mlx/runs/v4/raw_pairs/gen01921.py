from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.when(F.col("pressure") >= 1016, "High").when(F.col("pressure") >= 1004, "Medium").otherwise("Low").alias("pressure_band"),
    )
)

result = (
    filtered
    .groupBy("firmware")
    .pivot("device_code", ["DV-04", "DV-02"])
    .agg(F.max("status_bits").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "firmware",
        F.col("DV-04_amount").alias("dv_04_amount"),
        F.col("DV-04_n").alias("dv_04_n"),
        F.col("DV-02_amount").alias("dv_02_amount"),
        F.col("DV-02_n").alias("dv_02_n"),
    )
)

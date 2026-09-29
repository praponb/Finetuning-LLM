from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("temp_c") > 30)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") + F.col("status_bits")).alias("temp_c_plus_status_bits"),
    )
)

result = (
    cleaned
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-03"])
    .agg(F.max("temp_c_plus_status_bits").alias("total"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-03").alias("dv_03"))
)

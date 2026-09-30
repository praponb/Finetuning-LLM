from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.to_date("reading_ts") < "2024-04-01"))
    .select(
        "*",
        F.when(F.col("temp_c") >= 35.4, "High").when(F.col("temp_c") >= 24.1, "Medium").otherwise("Low").alias("temp_c_level"),
    )
)

result = (
    valid
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("humidity").between(64, 69))
        .select("id", "device_code")
    )
)

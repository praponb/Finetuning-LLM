from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits_adjusted", "id")
w2 = Window.orderBy(F.col("temp_c_per_pressure").desc_nulls_first())

cleaned = (
    df_device_readings
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("status_bits") * 1.1), 2).alias("status_bits_adjusted"),
    )
)

filtered = (
    cleaned
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00")))
    .select(
        "*",
        F.trim("firmware").alias("clean_firmware"),
        F.round((F.col("temp_c") / F.col("pressure")), 2).alias("temp_c_per_pressure"),
    )
)

cleaned_device_readings = (
    filtered
    .filter(F.col("humidity").isNotNull())
    .select(
        "id",
        "temp_c",
        "status_bits",
        "status_bits_adjusted",
        "clean_firmware",
        "temp_c_per_pressure",
        F.lower("firmware").alias("firmware_lower"),
    )
)

cleaned_device_readings_2 = (
    cleaned_device_readings
    .filter(F.col("temp_c_per_pressure") <= 55)
    .select(
        "id",
        "status_bits",
        "status_bits_adjusted",
        "clean_firmware",
        "temp_c_per_pressure",
        F.greatest("status_bits", "temp_c_per_pressure").alias("greatest_status_bits_temp_c_per_pressure"),
    )
)

result = (
    cleaned_device_readings_2
    .filter(F.col("temp_c_per_pressure") > 57)
    .select(
        "id",
        "clean_firmware",
        F.lead("greatest_status_bits_temp_c_per_pressure", 2).over(w).alias("next_greatest_status_bits_temp_c_per_pressure"),
        F.cume_dist().over(w2).alias("temp_c_per_pressure_cume_dist"),
    )
)

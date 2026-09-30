from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("temp_c") >= 33)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
    )
)

valid = (
    base
    .filter(F.col("reading_ts") >= "2024-02-15")
    .withColumn("temp_c_rounded", F.round("temp_c", 2))
)

cleaned = (
    valid
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("pressure") > 998))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "status_bits_plus_humidity",
        "temp_c_rounded",
        F.when(F.col("status_bits_plus_humidity") >= 69, "High").when(F.col("status_bits_plus_humidity") >= 18, "Medium").otherwise("Low").alias("status_bits_plus_humidity_level"),
    )
)

valid_device_readings = (
    cleaned
    .filter((F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 02:00:00"))) & (F.col("humidity") > 64))
    .select("*", F.trim("firmware").alias("clean_firmware"))
)

result = (
    valid_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.percentile_approx("status_bits_plus_humidity", 0.9).alias("status_bits_plus_humidity_p90"),
        F.min("temp_c_rounded").alias("min_temp_c_rounded"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.min("reading_ts").alias("first_seen"),
    )
    .filter(F.col("avg_temp_c") >= 0)
    .orderBy(F.col("min_temp_c_rounded").desc_nulls_last())
)

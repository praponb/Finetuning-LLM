from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("device_code") == "DV-04")
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("status_bits").cast("double").alias("status_bits_num"),
    )
)

kept_keys = (
    filtered
    .select("id", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("firmware") != "1.1.0")
        .select("id", "firmware")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
    .withColumn("pressure_scaled", F.round((F.col("pressure") * 1.1), 2))
)

filtered_device_readings = (
    valid
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "days_since_reading_ts",
        "pressure_scaled",
        F.round("pressure_scaled", 0).alias("pressure_scaled_rounded"),
    )
)

cleaned = (
    filtered_device_readings
    .filter(F.col("pressure_scaled_rounded").between(40, 64))
    .select(
        "id",
        "pressure",
        "firmware",
        "pressure_scaled",
        "pressure_scaled_rounded",
        F.least("pressure_scaled_rounded", "pressure").alias("least_pressure_scaled_rounded_pressure"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("pressure_scaled") >= 66, "LARGE").otherwise("SMALL").alias("pressure_scaled_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("least_pressure_scaled_rounded_pressure").alias("min_least_pressure_scaled_rounded_pressure"),
    )
    .filter(F.col("min_least_pressure_scaled_rounded_pressure") >= 0)
)

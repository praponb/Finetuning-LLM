from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp")) & (F.col("temp_c") <= 24))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        (F.col("pressure") + F.col("status_bits")).alias("pressure_plus_status_bits"),
    )
)

remaining_keys = (
    base
    .select("device_code", "id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure").between(995, 1005))
        .select("device_code", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("pressure") <= 996)
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "pressure_plus_status_bits",
        (F.col("pressure") - F.col("status_bits")).alias("pressure_minus_status_bits"),
        F.when(F.col("device_code") == "DV-03", 1).otherwise(0).alias("is_dv_03"),
    )
)

base_device_readings = (
    valid
    .filter(F.col("status_bits") <= 9)
    .select(
        "id",
        "humidity",
        "pressure",
        "pressure_minus_status_bits",
        "is_dv_03",
        F.ceil("pressure_minus_status_bits").alias("pressure_minus_status_bits_whole"),
        F.round("humidity", 0).alias("humidity_rounded"),
    )
)

result = (
    base_device_readings
    .groupBy(
        F.when(F.col("pressure_minus_status_bits") >= 87, "HIGH").otherwise("LOW").alias("pressure_minus_status_bits_tier"),
        F.when(F.col("pressure") >= 1018, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        F.when(F.col("pressure_minus_status_bits_whole") >= 82, "HIGH").otherwise("LOW").alias("pressure_minus_status_bits_whole_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("is_dv_03").alias("total_is_dv_03"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") < 1000)
)

from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") >= 2.2) & (F.col("valid_from") >= F.lit("2025-01-24 14:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

tariff_rows = base_tariffs.select(F.col("id").alias("key_id"))

reading_rows = df_meter_readings.select(F.col("id").alias("key_id"))

result = (
    tariff_rows
    .intersectAll(reading_rows)
    .orderBy("key_id")
)

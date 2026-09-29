from pyspark.sql import functions as F

valid_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_rows = (
    valid_tariffs
    .filter((F.col("tariff") == "TOU") & (F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp")))
    .select(
        F.lit("tariff").alias("origin"),
        F.col("tariff").alias("name"),
        F.col("id").alias("ref_id"),
    )
)

reading_side = df_meter_readings.select(F.lit("reading").alias("origin"), "quality", "id")

result = (
    tariff_rows
    .union(reading_side)
    .distinct()
)

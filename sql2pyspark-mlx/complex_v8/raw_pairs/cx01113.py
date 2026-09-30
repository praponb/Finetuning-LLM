from pyspark.sql import functions as F

scoped_tariffs = (
    df_tariff_windows
    .filter((~F.col("tariff").isin("EV", "FLAT")) & (F.col("valid_to") >= F.lit("2025-02-18 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_rows = (
    scoped_tariffs
    .filter((F.col("valid_from") >= F.lit("2025-01-23 19:00:00").cast("timestamp")) & (F.col("tariff").isNotNull()))
    .select(
        "id",
        "rate",
        F.col("valid_to").alias("happened_at"),
        F.lit("tariff").alias("source"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        "id",
        "voltage",
        "read_at",
        F.lit("reading").alias("source"),
    )
)

result = (
    tariff_rows
    .union(reading_side)
    .orderBy(F.col("happened_at").desc(), "source")
)

from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        (F.col("valid_to") + F.expr("INTERVAL 2 HOURS")).alias("valid_to_plus_2h"),
    )
)

tariff_side = (
    filtered_tariffs
    .filter(F.col("valid_to_plus_2h") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select("id", "valid_from")
)

reading_side = (
    df_meter_readings
    .filter(F.col("voltage") <= 235.8)
    .select("site_id", "read_at")
)

result = tariff_side.union(reading_side)

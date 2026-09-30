from pyspark.sql import functions as F

prepared = (
    df_tariff_windows
    .filter((F.col("rate") > 3.0) & (F.col("valid_to") >= F.lit("2025-02-16 05:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_rows = (
    prepared
    .filter((F.col("tariff") != "FLAT") & (F.col("rate") <= 6.1))
    .select(
        "tariff",
        "rate",
        F.col("valid_from").alias("event_time"),
        F.lit("tariff").alias("record_type"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        F.col("quality").alias("tariff"),
        F.col("voltage").alias("rate"),
        F.col("read_at").alias("event_time"),
        F.lit("reading").alias("record_type"),
    )
)

result = tariff_rows.unionByName(reading_side)

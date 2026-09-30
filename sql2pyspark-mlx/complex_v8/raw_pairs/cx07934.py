from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        F.col("site_id").alias("qty"),
        F.col("read_at").alias("event_time"),
        F.lit("reading").alias("origin"),
    )
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select("id", "valid_to", F.lit("tariff").alias("origin"))
)

result = reading_side.union(tariff_side)

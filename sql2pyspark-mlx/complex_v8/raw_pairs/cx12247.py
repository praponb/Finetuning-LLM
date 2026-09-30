from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate") < 5.5) & (F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp")))
    .select("id", F.col("valid_to").alias("event_time"))
)

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") > 52.4) & (F.col("site_id") <= 2))
    .select(F.col("site_id").alias("id"), F.col("read_at").alias("event_time"))
)

result = tariff_side.union(reading_side)

from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .select(
        "tariff",
        F.col("valid_to").alias("event_time"),
        F.lit("tariff").alias("source"),
    )
)

reading_side = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 9)) & (F.col("quality") == "OK"))
    .select("quality", "read_at", F.lit("reading").alias("source"))
)

result = tariff_rows.union(reading_side)

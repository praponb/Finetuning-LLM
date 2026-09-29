from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.lit("reading").alias("src"), "id", "read_at")

tariff_side = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select(F.lit("tariff").alias("src"), "id", "valid_from")
)

all_events = (
    reading_side
    .union(tariff_side)
    .distinct()
)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("read_at").alias("latest_read_at"),
    )
    .orderBy("src")
)

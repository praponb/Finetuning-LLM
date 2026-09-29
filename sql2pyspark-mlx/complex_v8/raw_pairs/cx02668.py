from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(
        "id",
        F.col("rate").alias("metric"),
        F.col("valid_to").alias("event_time"),
    )
)

reading_side = df_meter_readings.select("id", "kwh", "read_at")

combined = tariff_side.union(reading_side)

result = (
    combined
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("metric").alias("total_metric"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("id")
)

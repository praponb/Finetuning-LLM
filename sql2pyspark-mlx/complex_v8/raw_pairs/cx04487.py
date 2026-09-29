from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("quality") != "OK") | (F.col("voltage").between(220.2, 237.5)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_rows = (
    cleaned
    .filter(F.col("site_id").isNotNull())
    .select(
        F.col("site_id").alias("ref_id"),
        F.col("read_at").alias("event_time"),
        F.col("voltage").alias("metric"),
    )
)

reading_side = (
    df_meter_readings
    .select(
        F.col("id").alias("ref_id"),
        F.col("read_at").alias("event_time"),
        F.col("voltage").alias("metric"),
    )
)

combined = reading_rows.union(reading_side)

result = (
    combined
    .groupBy("ref_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("metric").alias("total_metric"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("ref_id")
)

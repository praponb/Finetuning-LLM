from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("kwh") <= 79.4)
    .select(
        F.lit("reading").alias("record_type"),
        "site_id",
        F.col("read_at").alias("event_time"),
        F.col("id").alias("qty"),
    )
)

reading_side_2 = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-02 13:00:00").cast("timestamp")) & (F.col("voltage") >= 221.4))
    .select(
        F.lit("reading_2").alias("record_type"),
        "site_id",
        F.col("read_at").alias("event_time"),
        F.col("id").alias("qty"),
    )
)

combined = reading_side.union(reading_side_2)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("record_type")
)

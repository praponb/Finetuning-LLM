from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter((F.col("plan").isNotNull()) & (F.col("signup_date") >= F.lit("2024-08-02").cast("date")))
    .select(
        F.col("id").alias("key_id"),
        F.col("signup_date").alias("event_date"),
        F.lit("listener").alias("record_type"),
    )
)

track_rows = (
    df_tracks
    .select(
        F.col("id").alias("key_id"),
        F.col("release_date").alias("event_date"),
        F.lit("track").alias("record_type"),
    )
)

stacked = (
    listener_rows
    .unionByName(track_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("record_type")
)

from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

play_rows = (
    valid
    .select(
        F.col("track_id").alias("entity_id"),
        F.col("played_at").alias("event_time"),
    )
)

play_side = (
    df_plays
    .filter((F.col("device") == "car") & (F.col("track_id") <= 19))
    .select("id", "played_at")
)

all_events = play_rows.union(play_side)

result = (
    all_events
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("entity_id")
)

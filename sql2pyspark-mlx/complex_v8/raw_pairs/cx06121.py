from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
    )
)

play_rows = (
    valid_plays
    .filter((F.col("device_group") != "other") & (F.col("listener_id") <= 17))
    .select(
        F.col("source").alias("category"),
        F.col("track_id").alias("ref_id"),
        F.col("played_at").alias("event_time"),
    )
)

play_rows_2 = (
    df_plays
    .filter(F.col("track_id") <= 15)
    .select(
        F.col("device").alias("category"),
        F.col("listener_id").alias("ref_id"),
        F.col("played_at").alias("event_time"),
    )
)

unioned = play_rows.union(play_rows_2)

result = (
    unioned
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("category")
)

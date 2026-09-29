from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

combined_details = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["device", "track_id", "listener_id"], ["device"], ["track_id"], []],
        "device", "track_id", "listener_id",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.max("track_duration_sec").alias("max_track_duration_sec"),
        F.round(F.stddev("track_duration_sec"), 2).alias("stddev_track_duration_sec"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "track_id",
        "listener_id",
        "distinct_listener_ids",
        "max_track_duration_sec",
        "stddev_track_duration_sec",
    )
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)

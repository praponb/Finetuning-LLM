from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("source").isin("radio", "search", "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_details_listener = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.device_prefix",
        "s.ms_played_bucket",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

with_parent_track = (
    with_details_listener.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.is_explicit").alias("track_is_explicit"))
)

result = (
    with_parent_track
    .groupBy("listener_id", "track_release_date")
    .pivot("listener_country", ["KR", "BR", "TH", "UK"])
    .agg(F.count("track_duration_sec"))
)

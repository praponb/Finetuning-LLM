from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") > 73335) & (F.col("played_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

combined_details_track = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_or_unknown",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.interests").alias("listener_interests"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") != "car", 1)).alias("play_device_count"),
        F.count(F.when(F.col("source") == "search", 1)).alias("play_source_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

with_counts = (
    combined_details_track.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.min_play_ms_played",
    )
)

result = (
    with_counts
    .groupingSets(
        [["is_skipped", "play_device_count"], ["is_skipped"], []],
        "is_skipped", "play_device_count",
    )
    .agg(
        F.sum(F.when(~F.col("is_skipped"), F.col("play_device_count")).otherwise(0)).alias("play_device_count_not_is_skipped"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("play_device_count").asc_nulls_last(),
    )
)

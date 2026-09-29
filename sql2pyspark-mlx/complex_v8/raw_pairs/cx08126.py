from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("source") == "album") | (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_device_label",
        "s.device_prefix",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") <= 318978) & (F.col("p.listener_id") <= 17), "left_semi")
    .filter(F.col("device_prefix").isNotNull())
)

result = (
    qualified
    .cube("source", "device")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_ms_played"))
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "avg_ms_played",
    )
)

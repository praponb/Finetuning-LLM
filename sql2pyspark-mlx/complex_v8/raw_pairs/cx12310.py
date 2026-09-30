from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

combined_details_listener = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.played_at_quarter",
        "s.device_prefix",
        F.col("l.country").alias("listener_country"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

with_children_plays = (
    combined_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.device_prefix",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

scored = (
    with_children_plays
    .filter(F.col("device_prefix").isNotNull())
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "device_prefix",
        "play_id",
        "play_ms_played",
        "play_device",
        F.when(F.col("ms_played") >= 287769, "HIGH").when(F.col("ms_played") >= 73335, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.concat_ws("-", F.col("device"), F.col("device_prefix")).alias("device_device_prefix_label"),
        F.abs(F.col("play_ms_played") - 223262).alias("play_ms_played_distance"),
    )
)

result = (
    scored
    .groupBy("play_id", "play_device")
    .pivot("ms_played_tier", ["HIGH", "LOW", "MEDIUM"])
    .agg(F.sum("ms_played").alias("total"), F.max("ms_played").alias("max_v"))
    .orderBy("play_id", "play_device")
)

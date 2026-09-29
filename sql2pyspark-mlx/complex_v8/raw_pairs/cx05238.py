from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) | (F.col("ms_played") > 95960))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_children_plays = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    with_children_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id"), "left")
    .select("s.*", "p.first_played_at")
)

derived = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "play_id",
        "play_track_id",
        F.lower(F.regexp_replace(F.trim(F.col("play_device")), " ", "_")).alias("play_device_slug"),
        F.length(F.trim(F.col("play_source"))).alias("play_source_len"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_parent_track = (
    derived.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.play_id",
        "s.play_track_id",
        "s.play_device_slug",
        "s.device_slug",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    with_parent_track
    .filter(F.col("track_title").isNotNull())
    .select(
        "id",
        "listener_id",
        "play_id",
        "play_track_id",
        "play_device_slug",
        "track_title",
    )
)

from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

stats = (
    recent
    .groupBy("track_id", "source", "listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("row_count"),
        F.max("ms_played_bucket").alias("max_ms_played_bucket"),
    )
)

joined = (
    stats.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.track_id",
        "s.source",
        "s.listener_id",
        "s.last_played_at",
        "s.row_count",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
    )
)

enriched = (
    joined
    .select(
        "*",
        F.abs(F.col("row_count") - 7).alias("row_count_distance"),
        (F.floor(F.col("track_bpm") / 25) * 25).alias("track_bpm_bucket"),
    )
)

exploded_detail_plays = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.track_id",
        "s.source",
        "s.listener_id",
        "s.last_played_at",
        "s.row_count",
        "s.track_is_explicit",
        "s.track_title",
        "s.row_count_distance",
        "s.track_bpm_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

exploded_detail = (
    exploded_detail_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.track_id",
        "s.source",
        "s.listener_id",
        "s.track_is_explicit",
        "s.row_count_distance",
        "s.play_id",
        "s.play_played_at",
        "s.play_source",
        F.col("p.id").alias("play_id_2"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at_2"),
    )
)

result = (
    exploded_detail
    .select(
        "track_id",
        "source",
        "listener_id",
        "track_is_explicit",
        "row_count_distance",
        "play_id",
        "play_played_at",
        "play_source",
        "play_id_2",
        "play_is_skipped_2",
        F.round(F.col("row_count_distance") / F.nullif(F.col("play_ms_played"), F.lit(0)), 3).alias("row_count_distance_per_play_ms_played"),
        F.concat_ws(" / ", F.col("source"), F.col("play_source")).alias("source_play_source_label"),
    )
)

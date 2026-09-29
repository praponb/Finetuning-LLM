from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.abs(F.col("ms_played") - 168782).alias("ms_played_distance"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source") == "album", 1)).alias("play_source_count"),
    )
)

with_stats = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

detail_rows = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        "s.ms_played_distance",
        "s.play_source_count",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
    )
)

with_parent_play = (
    detail_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        "s.play_source_count",
        "s.play_id",
        "s.play_played_at",
        "s.play_ms_played",
        "s.play_device",
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at_2"),
    )
)

source_stats = (
    with_parent_play
    .groupBy("source")
    .agg(F.avg("play_ms_played").alias("avg_play_ms_played"))
)

above_average = (
    with_parent_play.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.play_ms_played") > F.col("g.avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.avg_play_ms_played"), 2).alias("play_ms_played_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isNotNull()) & (F.col("p.ms_played") >= 56848), "left_anti")
)

result = (
    without_matches
    .groupBy("play_played_at")
    .pivot("device", ["car", "desktop", "phone"])
    .agg(F.sum("avg_play_ms_played").alias("amount"), F.max("avg_play_ms_played").alias("max_v"))
    .orderBy("play_played_at")
)

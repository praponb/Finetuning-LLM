from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter(F.col("ms_played") <= 318978)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)

detail_rows_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.played_at_plus_7d",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

play_device_stats = (
    detail_rows_plays
    .groupBy("play_device")
    .agg(F.avg("play_ms_played").alias("avg_play_ms_played"))
)

vs_group = (
    detail_rows_plays.alias("s")
    .join(play_device_stats.alias("g"), F.col("g.play_device") == F.col("s.play_device"), "inner")
    .filter(F.col("s.play_ms_played") > F.col("g.avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.avg_play_ms_played"), 2).alias("play_ms_played_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["play_track_id", "play_is_skipped"], ["play_track_id"], []],
        "play_track_id", "play_is_skipped",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("play_device").isNotNull(), 1)) / F.count("*"), 1).alias("pct_play_device_is_not_null"),
        F.percentile_approx("play_ms_played", 0.5).alias("median_play_ms_played"),
        F.round(F.sum("play_ms_played_minus_avg") / F.nullif(F.sum("play_ms_played"), F.lit(0)), 3).alias("play_ms_played_minus_avg_to_play_ms_played_ratio"),
    )
)

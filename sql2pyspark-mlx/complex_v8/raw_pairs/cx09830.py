from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("device").isin("phone", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

grouped = (
    scoped
    .groupBy("track_id", F.to_date("played_at").alias("played_at_day"))
    .agg(
        F.round(F.avg("ms_played_filled"), 2).alias("avg_ms_played_filled"),
        F.count(F.when(F.col("device") == "car", 1)).alias("n_device_car"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.round(F.sum("ms_played_filled") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("ms_played_filled_to_ms_played_ratio"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.avg("ms_played_filled").alias("avg_ms_played_filled_check"),
    )
    .filter(F.col("avg_ms_played_filled_check") > 44500)
    .drop("avg_ms_played_filled_check")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(F.col("source") == "album", 1)).alias("play_source_count"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    grouped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count_2"))
)

with_stats_2 = (
    with_stats.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    with_stats_2
    .select(
        "track_id",
        "played_at_day",
        "n_device_car",
        "median_ms_played",
        "ms_played_filled_to_ms_played_ratio",
        "last_played_at",
        "play_source_count",
        "play_count",
    )
    .orderBy("ms_played_filled_to_ms_played_ratio", "track_id", "played_at_day")
    .limit(10)
)

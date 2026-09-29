from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.min("ms_played").alias("min_play_ms_played"),
        F.count(F.when(F.col("device") != "speaker", 1)).alias("play_device_count"),
    )
)

with_counts = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

rollup_base = (
    with_counts
    .groupBy(
        "play_device_count",
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "listener_id",
    )
    .agg(
        F.sum("play_device_count").alias("total_play_device_count"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("min_play_ms_played"), 2).alias("stddev_min_play_ms_played"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
)

profiled = (
    rollup_base.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.first_played_at")
)

result = (
    profiled
    .filter(F.col("total_play_device_count") <= 34)
    .select(
        "play_device_count",
        "played_at_month",
        "listener_id",
        "total_play_device_count",
        "cnt",
        "stddev_min_play_ms_played",
        "max_play_ms_played",
        "first_played_at",
        F.abs(F.col("max_play_ms_played") - 261750).alias("max_play_ms_played_distance"),
    )
    .orderBy(
        F.col("stddev_min_play_ms_played").desc_nulls_first(),
        "max_play_ms_played",
    )
)

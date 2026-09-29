from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((F.col("device") == "speaker") & (F.col("ms_played") <= 301149))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.abs(F.col("ms_played") - 166517).alias("ms_played_distance"),
    )
)

totals = (
    cleaned
    .groupBy("is_skipped", "listener_id", "device_or_unknown")
    .agg(
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.sum("ms_played_distance").alias("total_ms_played_distance"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.count(F.when(~F.col("device").isin("speaker", "car"), 1)).alias("n_device_not_in_speaker_car"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 51883)
    .drop("avg_ms_played_check")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    totals.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()) & (F.col("p.is_skipped")), "left_anti")
)

result = (
    qualified
    .rollup("play_count", "distinct_track_ids", "is_skipped")
    .agg(
        F.round(F.avg("min_play_ms_played"), 2).alias("avg_min_play_ms_played"),
        F.round(F.avg("n_device_not_in_speaker_car"), 2).alias("avg_n_device_not_in_speaker_car"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true"),
    )
)

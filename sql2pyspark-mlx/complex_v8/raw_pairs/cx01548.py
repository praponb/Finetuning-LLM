from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

qualified = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") <= 18), "left_anti")
    .filter(F.month(F.col("played_at")).isin(2, 3))
)

per_group = (
    qualified
    .groupBy("track_id", "device", "listener_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("device") != "tv", 1)).alias("n_device_tv"),
        F.sum(F.when(F.col("device") == "tv", 1).otherwise(0)).alias("n_device_tv_2"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") == "car", 1)).alias("play_device_count"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    per_group.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    with_stats
    .filter(F.col("n_is_skipped_true") >= 2)
    .select(
        "track_id",
        "device",
        "row_count",
        "n_device_tv_2",
        "play_device_count",
        "play_is_skipped_count",
        "play_count",
        F.round(F.col("n_device_tv") / F.nullif(F.col("play_count"), F.lit(0)), 3).alias("n_device_tv_per_play_count"),
    )
    .orderBy(
        F.col("play_is_skipped_count").desc(),
        F.col("n_device_tv_per_play_count").desc_nulls_first(),
    )
)

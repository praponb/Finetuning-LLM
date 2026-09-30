from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("listener_id") >= 7) & (F.col("played_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count_2"),
    )
)

summarized_rows = (
    scoped_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

tagged = (
    summarized_rows
    .withColumn("played_at_label_3", F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00"))
)

tagged_2 = (
    tagged
    .select(
        "*",
        F.when(F.col("play_is_skipped_count") >= 5, "large").when(F.col("play_is_skipped_count") >= 2, "medium").otherwise("small").alias("play_is_skipped_count_tier"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

result = (
    tagged_2
    .groupingSets(
        [["play_is_skipped_count", "play_count_2", "device_or_unknown"], ["play_is_skipped_count"], ["play_count_2"], []],
        "play_is_skipped_count", "play_count_2", "device_or_unknown",
    )
    .agg(
        F.sum("play_is_skipped_count").alias("total_play_is_skipped_count"),
        F.round(F.sum("play_is_skipped_count") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("play_is_skipped_count_to_ms_played_ratio"),
    )
)

from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")) & (F.col("device") == "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 301865, "large").when(F.col("ms_played") >= 71476, "medium").otherwise("small").alias("ms_played_tier"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

grouped = (
    filtered_plays
    .filter(F.col("source_len") >= 4)
    .groupBy("track_id", "source_len")
    .agg(
        F.round(F.sum("source_len") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("source_len_to_ms_played_ratio"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.sum(F.when(~F.col("is_skipped"), 1).otherwise(0)).alias("n_not_is_skipped"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 56858)
    .drop("avg_ms_played_check")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

with_counts = (
    grouped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

result = (
    with_counts
    .filter(F.col("source_len_to_ms_played_ratio") < 6.0)
    .select(
        "track_id",
        "source_len_to_ms_played_ratio",
        "device_list",
        "source_list",
        "n_not_is_skipped",
    )
)

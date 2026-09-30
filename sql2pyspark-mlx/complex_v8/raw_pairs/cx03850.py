from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter(F.col("source") == "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug_2"),
    )
)

stats = (
    cleaned
    .groupBy("listener_id", "source")
    .agg(
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.sum(F.when(F.col("source").isin("search", "album"), F.col("ms_played"))).alias("ms_played_source_in_search_album"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("play_source_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

summarized_rows = (
    stats.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.max_play_ms_played",
        "p.avg_play_ms_played",
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

result = (
    with_counts
    .groupBy("first_played_at")
    .pivot("source", ["album", "search", "playlist"])
    .agg(F.min("avg_play_ms_played"))
    .orderBy("first_played_at")
)

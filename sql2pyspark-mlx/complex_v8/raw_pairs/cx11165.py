from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.abs(F.col("ms_played") - 168782).alias("ms_played_distance"),
    )
)

screened = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isNotNull()), "left_anti")
)

filtered = (
    screened
    .crossJoin(screened.agg(F.min("ms_played_distance").alias("overall_min_ms_played_distance")))
    .filter(F.col("ms_played_distance") > F.col("overall_min_ms_played_distance"))
    .drop("overall_min_ms_played_distance")
)

scored = (
    filtered
    .select(
        "*",
        (F.floor(F.col("ms_played_distance") / 50000) * 50000).alias("ms_played_distance_bucket"),
        F.coalesce(F.col("ms_played_distance"), F.lit(0)).alias("ms_played_distance_filled"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count("*").alias("play_count_3"),
    )
)

with_stats = (
    scored.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_count_3"), F.lit(0)).alias("play_count_3"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("source").isin("search", "playlist"), 1)).alias("play_source_count"),
    )
)

with_counts = (
    with_stats.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

result = (
    with_counts
    .groupBy("first_played_at")
    .pivot("device_or_unknown", ["tv", "phone"])
    .agg(F.min("ms_played_distance"))
)

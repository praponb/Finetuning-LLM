from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("source").isin("playlist", "search")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.abs(F.col("ms_played") - 241585).alias("ms_played_distance"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count(F.when(F.col("device") == "phone", 1)).alias("play_device_count"),
    )
)

with_stats = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

enriched = (
    with_stats
    .filter(F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref_2"),
    )
)

result = (
    enriched
    .groupingSets(
        [["listener_id", "play_device_count"], ["listener_id"]],
        "listener_id", "play_device_count",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.max("play_count_2").alias("max_play_count_2"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("play_device_count").asc_nulls_last(),
    )
)

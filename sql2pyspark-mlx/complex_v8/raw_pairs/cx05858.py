from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

tagged = (
    profiled
    .filter(F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("device").isin("desktop", "speaker"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
        F.least(F.col("ms_played"), F.col("play_count")).alias("least_ms_played_play_count"),
    )
)

result = (
    tagged
    .cube("device", "track_id", "device_group")
    .agg(
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
    )
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("device_group").asc_nulls_last(),
    )
)

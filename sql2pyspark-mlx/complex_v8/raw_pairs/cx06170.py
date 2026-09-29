from pyspark.sql import functions as F

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

stats = (
    matched
    .groupBy("listener_id", "prior_rows_3d")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("prior_rows_3d"), 2).alias("avg_prior_rows_3d"),
        F.round(F.sum("prior_ms_played_3d") / F.nullif(F.sum("prior_rows_3d"), F.lit(0)), 3).alias("prior_ms_played_3d_to_prior_rows_3d_ratio"),
        F.max("prior_ms_played_3d").alias("max_prior_ms_played_3d_check"),
    )
    .filter(F.col("max_prior_ms_played_3d_check") > 400000)
    .drop("max_prior_ms_played_3d_check")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("device").isin("desktop", "speaker"), 1)).alias("play_device_count"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    stats.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    with_counts
    .filter(F.col("first_played_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp"))
    .select(
        "listener_id",
        "prior_rows_3d",
        "first_played_at",
        "avg_prior_rows_3d",
        "play_device_count",
        "last_played_at",
    )
    .orderBy(F.col("listener_id").asc(), "prior_rows_3d")
    .limit(5)
)

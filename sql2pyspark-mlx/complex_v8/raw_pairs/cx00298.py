from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter((~F.col("is_skipped")) | (F.col("device") != "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.total_play_ms_played",
    )
)

result = (
    with_stats
    .groupBy("play_count", "listener_id")
    .pivot("device", ["speaker", "tv"])
    .agg(F.avg("ms_played").alias("avg_v"), F.sum("ms_played").alias("sum_v"))
    .orderBy("play_count", "listener_id")
)

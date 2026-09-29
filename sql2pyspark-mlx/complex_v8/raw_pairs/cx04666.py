from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("track_id") <= 12)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

with_stats = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    with_stats
    .groupBy("played_at")
    .pivot("device", ["speaker", "car"])
    .agg(F.count("play_count").alias("count_v"), F.sum("play_count").alias("sum_v"))
    .orderBy("played_at")
)

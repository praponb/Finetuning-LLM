from pyspark.sql import functions as F

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.ms_played").alias("prior_ms_played_1d"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("device") == "phone", 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    in_window.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("prior_ms_played_1d") < 1200000)
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_1d",
        "prior_ms_played_1d",
        "play_count",
        "play_device_count",
        (F.col("play_device_count") + F.col("play_count")).alias("play_device_count_plus_play_count"),
    )
    .orderBy("played_at", F.col("play_count").asc())
)

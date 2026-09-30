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

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

profiled = (
    matched.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

result = (
    profiled
    .select(
        "id",
        "listener_id",
        "prior_ms_played_3d",
        "avg_play_ms_played",
        F.round(F.col("avg_play_ms_played") * 1.5, 2).alias("avg_play_ms_played_adj"),
    )
    .orderBy("listener_id", F.col("id").asc_nulls_last())
)

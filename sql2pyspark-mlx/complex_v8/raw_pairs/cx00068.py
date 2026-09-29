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

detail_rows_plays = (
    matched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source") == "radio"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_ms_played_3d",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    detail_rows_plays
    .groupingSets(
        [["listener_id", "play_source"], ["listener_id"], []],
        "listener_id", "play_source",
    )
    .agg(
        F.sum("prior_ms_played_3d").alias("total_prior_ms_played_3d"),
        F.round(F.stddev("prior_ms_played_3d"), 2).alias("stddev_prior_ms_played_3d"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("play_source").asc_nulls_last(),
    )
)

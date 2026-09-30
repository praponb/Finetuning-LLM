from pyspark.sql import functions as F

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

exploded_detail = (
    in_window.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_7d",
        "s.prior_ms_played_7d",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "listener_id",
        "prior_rows_7d",
        "prior_ms_played_7d",
        "play_id",
        "play_device",
        "play_is_skipped",
        "play_source",
        F.greatest(F.col("prior_ms_played_7d"), F.col("prior_rows_7d")).alias("greatest_prior_ms_played_7d_prior_rows_7d"),
    )
    .orderBy(F.col("prior_ms_played_7d").desc())
)

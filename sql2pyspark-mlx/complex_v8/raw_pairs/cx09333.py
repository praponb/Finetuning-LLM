from pyspark.sql import functions as F

covered = (
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

joined_listener = (
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_ms_played_1d",
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    joined_listener
    .select(
        "id",
        "played_at",
        "prior_ms_played_1d",
        "listener_username",
    )
    .orderBy(F.col("id").desc(), "played_at")
)

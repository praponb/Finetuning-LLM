from pyspark.sql import functions as F

covered = (
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

enriched = (
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    enriched
    .filter(F.col("played_at") < F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .select(
        "id",
        "played_at",
        "prior_rows_3d",
        "listener_plan",
    )
)

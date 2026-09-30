from pyspark.sql import functions as F

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

enriched = (
    covered
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_12h",
        "prior_ms_played_12h",
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
        F.least(F.col("prior_ms_played_12h"), F.col("prior_rows_12h")).alias("least_prior_ms_played_12h_prior_rows_12h"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "prior_rows_12h",
        "prior_ms_played_12h",
        "played_at_plus_2h",
        "days_since_played_at",
        F.abs(F.col("prior_rows_12h") - 6).alias("prior_rows_12h_distance"),
    )
    .orderBy(F.col("prior_rows_12h").desc(), "id")
)

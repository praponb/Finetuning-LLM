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

tagged = (
    covered
    .select(
        "*",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

result = (
    tagged
    .filter(F.col("prior_rows_1d").between(2, 6))
    .select(
        "id",
        "listener_id",
        "prior_rows_1d",
        "prior_ms_played_1d",
        "played_at_hour",
        F.least(F.col("prior_ms_played_1d"), F.col("played_at_hour")).alias("least_prior_ms_played_1d_played_at_hour"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

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

stats = (
    in_window
    .groupBy(
        "listener_id",
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "prior_rows_7d",
    )
    .agg(
        F.round(F.stddev("prior_rows_7d"), 2).alias("stddev_prior_rows_7d"),
        F.min("played_at").alias("first_played_at"),
        F.min("prior_ms_played_7d").alias("min_prior_ms_played_7d"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select(
        "listener_id",
        "played_at_month",
        "prior_rows_7d",
        "stddev_prior_rows_7d",
        "first_played_at",
        "min_prior_ms_played_7d",
        "cnt",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_played_at")).alias("days_since_first_played_at"),
        (F.col("stddev_prior_rows_7d") + F.col("cnt")).alias("stddev_prior_rows_7d_plus_cnt"),
    )
)

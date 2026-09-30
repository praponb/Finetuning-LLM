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

grouped = (
    covered
    .groupBy(
        "listener_id",
        "prior_rows_12h",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.min("prior_rows_12h").alias("min_prior_rows_12h"),
        F.round(F.stddev("prior_rows_12h"), 2).alias("stddev_prior_rows_12h"),
        F.min("played_at").alias("first_played_at"),
        F.max("prior_rows_12h").alias("max_prior_rows_12h_check"),
    )
    .filter(F.col("max_prior_rows_12h_check") > 2)
    .drop("max_prior_rows_12h_check")
)

scored = (
    grouped
    .select(
        "listener_id",
        "prior_rows_12h",
        "played_at_month",
        "min_prior_rows_12h",
        "first_played_at",
        F.date_format(F.col("first_played_at"), "yyyy-MM-dd HH:00").alias("first_played_at_label"),
    )
)

result = (
    scored
    .select(
        "listener_id",
        "prior_rows_12h",
        "played_at_month",
        "min_prior_rows_12h",
        "first_played_at",
        "first_played_at_label",
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
    )
    .orderBy("played_at_month", "listener_id", "prior_rows_12h")
    .limit(5)
)

from pyspark.sql import functions as F

in_window = (
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

rollup_base = (
    in_window
    .filter(F.col("prior_ms_played_3d") >= 200000)
    .groupBy(
        "prior_rows_3d",
        "listener_id",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.percentile_approx("prior_rows_3d", 0.5).alias("median_prior_rows_3d"),
        F.round(F.sum("prior_rows_3d") / F.nullif(F.sum("prior_ms_played_3d"), F.lit(0)), 3).alias("prior_rows_3d_to_prior_ms_played_3d_ratio"),
        F.max("played_at").alias("last_played_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("prior_rows_3d").alias("max_prior_rows_3d"),
        F.avg("prior_rows_3d").alias("avg_prior_rows_3d_check"),
    )
    .filter(F.col("avg_prior_rows_3d_check") > 1)
    .drop("avg_prior_rows_3d_check")
)

with_details = (
    rollup_base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.plan").isin("family", "student", "premium"))
    .select(
        "s.prior_rows_3d",
        "s.listener_id",
        "s.played_at_month",
        "s.median_prior_rows_3d",
        "s.distinct_ids",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    with_details
    .select(
        "prior_rows_3d",
        "listener_id",
        "listener_signup_date",
        "listener_plan",
    )
    .orderBy("listener_signup_date")
)

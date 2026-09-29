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

with_parent_listener = (
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_12h",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

listener_id_stats = (
    with_parent_listener
    .groupBy("listener_id")
    .agg(F.avg("prior_rows_12h").alias("avg_prior_rows_12h"))
)

outliers = (
    with_parent_listener.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.prior_rows_12h") > F.col("g.avg_prior_rows_12h"))
    .select(
        "s.*",
        "g.avg_prior_rows_12h",
        F.round(F.col("s.prior_rows_12h") - F.col("g.avg_prior_rows_12h"), 2).alias("prior_rows_12h_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "listener_id",
        "played_at",
        "listener_signup_date",
        "avg_prior_rows_12h",
        "prior_rows_12h_minus_avg",
        F.coalesce(F.col("avg_prior_rows_12h"), F.lit(0.0)).alias("avg_prior_rows_12h_filled"),
    )
    .orderBy(F.col("avg_prior_rows_12h").desc(), "id")
    .limit(20)
)

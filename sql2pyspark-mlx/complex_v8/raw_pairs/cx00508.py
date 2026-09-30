from pyspark.sql import functions as F

covered = (
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

combined_details = (
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.birth_year") < 1988)
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_7d",
        "s.prior_ms_played_7d",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

tagged = (
    combined_details
    .select(
        "*",
        (F.col("listener_birth_year") - F.col("prior_rows_7d")).alias("listener_birth_year_minus_prior_rows_7d"),
        (F.col("prior_rows_7d") + F.col("prior_ms_played_7d")).alias("prior_rows_7d_plus_prior_ms_played_7d"),
        F.greatest(F.col("listener_birth_year"), F.col("prior_rows_7d")).alias("greatest_listener_birth_year_prior_rows_7d"),
    )
)

result = (
    tagged
    .filter(F.col("prior_rows_7d_plus_prior_ms_played_7d") > 200001)
    .select(
        "id",
        "prior_rows_7d",
        "prior_ms_played_7d",
        "listener_birth_year",
        "listener_signup_date",
        "listener_birth_year_minus_prior_rows_7d",
        "prior_rows_7d_plus_prior_ms_played_7d",
    )
    .orderBy("prior_rows_7d_plus_prior_ms_played_7d", "id")
    .limit(10)
)

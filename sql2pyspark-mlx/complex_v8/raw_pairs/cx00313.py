from pyspark.sql import functions as F

joined_ranges = (
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

with_parent = (
    joined_ranges.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isin("KR", "TH")), "inner")
    .select("s.*", F.col("l.username").alias("listener_username"))
)

enriched_listener = (
    with_parent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select("s.*", F.col("l.username").alias("listener_username_2"))
)

with_parent_listener = (
    enriched_listener.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date").between(F.lit("2024-04-16").cast("date"), F.lit("2025-07-08").cast("date"))), "inner")
    .filter(F.size(F.col("l.interests")) >= 2)
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_1d",
        "s.prior_ms_played_1d",
        "s.listener_username_2",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    with_parent_listener
    .select(
        "id",
        "listener_username_2",
        "listener_signup_date",
        F.round(F.col("prior_ms_played_1d") / F.nullif(F.col("prior_rows_1d"), F.lit(0)), 3).alias("prior_ms_played_1d_per_prior_rows_1d"),
    )
    .orderBy(
        F.col("listener_username_2").desc(),
        F.col("prior_ms_played_1d_per_prior_rows_1d").asc_nulls_last(),
    )
)

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

enriched_listener = (
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.birth_year") > 1966)
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_12h",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

without_matches = (
    enriched_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")) & (F.col("p.source").isin("search", "radio", "album")), "left_anti")
)

rollup_base = (
    without_matches
    .filter(F.col("prior_rows_12h") < 6)
    .groupBy(
        "prior_rows_12h",
        F.to_date("played_at").alias("played_at_day"),
        "listener_id",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.min("played_at").alias("first_played_at"),
    )
)

result = (
    rollup_base
    .filter(F.col("listener_id") >= 16)
    .select(
        "prior_rows_12h",
        "played_at_day",
        "listener_id",
        "distinct_listener_ids",
        "first_played_at",
        F.least(F.col("distinct_listener_ids"), F.col("prior_rows_12h")).alias("least_distinct_listener_ids_prior_rows_12h"),
    )
    .orderBy(F.col("played_at_day").asc(), "distinct_listener_ids")
)

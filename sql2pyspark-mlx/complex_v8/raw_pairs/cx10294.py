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

with_details_listener = (
    joined_ranges.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

listener_id_stats = (
    with_details_listener
    .groupBy("listener_id")
    .agg(F.max("prior_rows_1d").alias("max_prior_rows_1d"))
)

above_average = (
    with_details_listener.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.prior_rows_1d") == F.col("g.max_prior_rows_1d"))
    .select(
        "s.*",
        "g.max_prior_rows_1d",
        F.round(F.col("s.prior_rows_1d") - F.col("g.max_prior_rows_1d"), 2).alias("prior_rows_1d_minus_max"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

summarized_rows = (
    above_average.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at", "p.total_play_ms_played")
)

result = (
    summarized_rows
    .filter(F.col("prior_rows_1d") >= 3)
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_1d",
        "prior_ms_played_1d",
        "listener_signup_date",
        "listener_plan",
        "listener_interests",
        "prior_rows_1d_minus_max",
        "last_played_at",
        "total_play_ms_played",
        F.lower(F.trim(F.col("listener_plan"))).alias("listener_plan_clean"),
        F.greatest(F.col("total_play_ms_played"), F.col("prior_rows_1d")).alias("greatest_total_play_ms_played_prior_rows_1d"),
    )
    .orderBy(
        F.col("prior_rows_1d_minus_max").desc_nulls_first(),
        F.col("played_at").desc_nulls_first(),
    )
)

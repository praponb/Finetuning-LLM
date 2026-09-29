from pyspark.sql import functions as F

joined_ranges = (
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

line_level = (
    joined_ranges.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_12h",
        "s.prior_ms_played_12h",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

qualified = (
    line_level.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")), "left_anti")
    .filter(F.col("played_at") >= F.lit("2025-01-19 06:00:00").cast("timestamp"))
)

result = (
    qualified
    .select(
        "id",
        "listener_id",
        "prior_rows_12h",
        "prior_ms_played_12h",
        "play_ms_played",
        "play_is_skipped",
        (F.col("prior_rows_12h") + F.col("prior_ms_played_12h")).alias("prior_rows_12h_plus_prior_ms_played_12h"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
    .orderBy(
        "prior_ms_played_12h",
        F.col("prior_rows_12h_plus_prior_ms_played_12h").desc(),
    )
)

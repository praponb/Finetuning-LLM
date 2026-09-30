from pyspark.sql import functions as F

joined_ranges = (
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

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("ms_played").alias("min_play_ms_played"))
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.min_play_ms_played")
)

result = (
    summarized_rows
    .select(
        "id",
        "played_at",
        "prior_rows_7d",
        "prior_ms_played_7d",
        "min_play_ms_played",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
    .orderBy(F.col("days_since_played_at").desc(), F.col("played_at_date").desc())
)

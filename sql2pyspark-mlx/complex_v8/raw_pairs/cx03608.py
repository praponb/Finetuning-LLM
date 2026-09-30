from pyspark.sql import functions as F

joined_ranges = (
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

filtered = (
    joined_ranges.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("D%")) & (F.col("l.plan") != "free"), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
    )
)

profiled = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

scored = (
    profiled
    .filter(F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.abs(F.col("prior_rows_3d") - 5).alias("prior_rows_3d_distance"),
    )
)

result = (
    scored
    .groupingSets(
        [["play_count", "prior_rows_3d_distance", "listener_id"], ["play_count"], ["prior_rows_3d_distance"], []],
        "play_count", "prior_rows_3d_distance", "listener_id",
    )
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("row_count"),
        F.min("played_at").alias("first_played_at"),
    )
    .orderBy(
        F.col("play_count").asc_nulls_last(),
        F.col("prior_rows_3d_distance").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)

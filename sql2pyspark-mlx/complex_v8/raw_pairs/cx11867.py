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

detail_rows = (
    covered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_12h",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

rollup_base = (
    detail_rows
    .groupBy("play_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("play_track_id").alias("distinct_play_track_ids"),
    )
)

result = (
    rollup_base
    .select(
        "play_id",
        "n_rows",
        "distinct_play_track_ids",
        F.round(F.col("n_rows") / F.nullif(F.col("distinct_play_track_ids"), F.lit(0)), 3).alias("n_rows_per_distinct_play_track_ids"),
        F.abs(F.col("distinct_play_track_ids") - 6).alias("distinct_play_track_ids_distance"),
    )
    .orderBy(F.col("distinct_play_track_ids").desc(), "play_id")
    .limit(20)
)

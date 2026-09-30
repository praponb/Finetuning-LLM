from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
    )
)

with_children = (
    prepared_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.played_at_month",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

totals = (
    with_children
    .filter(F.col("play_played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .groupBy(
        F.date_format("play_played_at", "yyyy-MM").alias("play_played_at_ym"),
        "played_at_month",
        "source",
    )
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.sum(F.when(F.col("play_is_skipped"), F.col("play_ms_played")).otherwise(0)).alias("play_ms_played_play_is_skipped_true"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
)

result = (
    totals
    .filter(F.col("source").isin("search", "album"))
    .select(
        "play_played_at_ym",
        "played_at_month",
        "total_ms_played",
        "play_ms_played_play_is_skipped_true",
        "source_list",
        (F.col("played_at_month") - F.col("total_ms_played")).alias("played_at_month_minus_total_ms_played"),
        F.when(F.col("played_at_month") >= 10, "large").when(F.col("played_at_month") >= 4, "medium").otherwise("small").alias("played_at_month_tier"),
    )
    .orderBy(F.col("played_at_month_minus_total_ms_played").desc())
)

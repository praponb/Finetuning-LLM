from pyspark.sql import functions as F

desktop_monthly = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

speaker_monthly = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("speaker_count"),
        F.round(F.avg("ms_played"), 2).alias("speaker_avg_ms_played"),
    )
)

result = (
    desktop_monthly.alias("a")
    .join(speaker_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.coalesce(F.col("a.desktop_total_ms_played"), F.lit(0)).alias("desktop_total_ms_played"),
        F.coalesce(F.col("b.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.coalesce(F.col("b.speaker_avg_ms_played"), F.lit(0)).alias("speaker_avg_ms_played"),
        (F.coalesce(F.col("a.desktop_count"), F.lit(0)) - F.coalesce(F.col("b.speaker_count"), F.lit(0))).alias("desktop_count_minus_speaker_count"),
    )
    .orderBy("played_at_month")
)

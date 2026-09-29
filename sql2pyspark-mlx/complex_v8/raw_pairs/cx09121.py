from pyspark.sql import functions as F

speaker_monthly = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("speaker_count"),
        F.sum("ms_played").alias("speaker_total_ms_played"),
    )
)

phone_monthly = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("phone_count"),
        F.round(F.avg("ms_played"), 2).alias("phone_avg_ms_played"),
    )
)

result = (
    speaker_monthly.alias("a")
    .join(phone_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.col("a.speaker_total_ms_played"),
        F.coalesce(F.col("b.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("b.phone_avg_ms_played"),
        (F.coalesce(F.col("a.speaker_count"), F.lit(0)) - F.coalesce(F.col("b.phone_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("played_at_month")
)

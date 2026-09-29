from pyspark.sql import functions as F

phone_monthly = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("phone_count"),
        F.round(F.avg("ms_played"), 2).alias("phone_avg_ms_played"),
    )
)

desktop_monthly = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("desktop_count"),
        F.round(F.avg("ms_played"), 2).alias("desktop_avg_ms_played"),
    )
)

result = (
    phone_monthly.alias("a")
    .join(desktop_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("a.phone_avg_ms_played"),
        F.coalesce(F.col("b.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.col("b.desktop_avg_ms_played"),
        (F.coalesce(F.col("a.phone_count"), F.lit(0)) - F.coalesce(F.col("b.desktop_count"), F.lit(0))).alias("phone_count_minus_desktop_count"),
    )
    .orderBy("played_at_month")
)

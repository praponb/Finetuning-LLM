from pyspark.sql import functions as F

car_monthly = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("car_count"),
        F.sum("ms_played").alias("car_total_ms_played"),
    )
)

desktop_monthly = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

result = (
    car_monthly.alias("a")
    .join(desktop_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.car_count"), F.lit(0)).alias("car_count"),
        F.coalesce(F.col("a.car_total_ms_played"), F.lit(0)).alias("car_total_ms_played"),
        F.coalesce(F.col("b.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.coalesce(F.col("b.desktop_total_ms_played"), F.lit(0)).alias("desktop_total_ms_played"),
        (F.coalesce(F.col("a.car_count"), F.lit(0)) - F.coalesce(F.col("b.desktop_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("played_at_month")
)

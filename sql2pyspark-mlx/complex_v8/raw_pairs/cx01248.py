from pyspark.sql import functions as F

desktop_monthly = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("desktop_count"),
        F.round(F.avg("ms_played"), 2).alias("desktop_avg_ms_played"),
    )
)

car_monthly = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.count("*").alias("car_count"),
        F.round(F.avg("ms_played"), 2).alias("car_avg_ms_played"),
    )
)

result = (
    desktop_monthly.alias("a")
    .join(car_monthly.alias("b"), F.col("a.played_at_month") == F.col("b.played_at_month"), "full")
    .select(
        F.coalesce(F.col("a.played_at_month"), F.col("b.played_at_month")).alias("played_at_month"),
        F.coalesce(F.col("a.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.col("a.desktop_avg_ms_played"),
        F.coalesce(F.col("b.car_count"), F.lit(0)).alias("car_count"),
        F.col("b.car_avg_ms_played"),
        (F.coalesce(F.col("a.desktop_count"), F.lit(0)) - F.coalesce(F.col("b.car_count"), F.lit(0))).alias("desktop_count_minus_car_count"),
    )
    .orderBy("played_at_month")
)

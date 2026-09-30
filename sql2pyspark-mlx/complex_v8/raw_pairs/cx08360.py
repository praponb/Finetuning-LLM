from pyspark.sql import functions as F

car_by_listener = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("car_count"),
        F.round(F.avg("ms_played"), 2).alias("car_avg_ms_played"),
    )
)

desktop_by_listener = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

result = (
    car_by_listener.alias("a")
    .join(desktop_by_listener.alias("b"), F.col("a.listener_id") == F.col("b.listener_id"), "full")
    .select(
        F.coalesce(F.col("a.listener_id"), F.col("b.listener_id")).alias("listener_id"),
        F.coalesce(F.col("a.car_count"), F.lit(0)).alias("car_count"),
        F.col("a.car_avg_ms_played"),
        F.coalesce(F.col("b.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.col("b.desktop_total_ms_played"),
        (F.coalesce(F.col("a.car_count"), F.lit(0)) - F.coalesce(F.col("b.desktop_count"), F.lit(0))).alias("car_count_minus_desktop_count"),
    )
    .orderBy("listener_id")
)

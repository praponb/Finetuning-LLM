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

phone_by_listener = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.round(F.avg("ms_played"), 2).alias("phone_avg_ms_played"),
    )
)

result = (
    car_by_listener.alias("a")
    .join(phone_by_listener.alias("b"), F.col("a.listener_id") == F.col("b.listener_id"), "full")
    .select(
        F.coalesce(F.col("a.listener_id"), F.col("b.listener_id")).alias("listener_id"),
        F.coalesce(F.col("a.car_count"), F.lit(0)).alias("car_count"),
        F.coalesce(F.col("a.car_avg_ms_played"), F.lit(0)).alias("car_avg_ms_played"),
        F.coalesce(F.col("b.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("b.phone_avg_ms_played"),
        (F.coalesce(F.col("a.car_count"), F.lit(0)) - F.coalesce(F.col("b.phone_count"), F.lit(0))).alias("car_count_minus_phone_count"),
    )
    .orderBy("listener_id")
)

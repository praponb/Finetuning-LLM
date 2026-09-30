from pyspark.sql import functions as F

phone_by_track = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.sum("ms_played").alias("phone_total_ms_played"),
    )
)

car_by_track = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("car_count"),
        F.sum("ms_played").alias("car_total_ms_played"),
    )
)

result = (
    phone_by_track.alias("a")
    .join(car_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("a.phone_total_ms_played"),
        F.coalesce(F.col("b.car_count"), F.lit(0)).alias("car_count"),
        F.coalesce(F.col("b.car_total_ms_played"), F.lit(0)).alias("car_total_ms_played"),
        (F.coalesce(F.col("a.phone_count"), F.lit(0)) - F.coalesce(F.col("b.car_count"), F.lit(0))).alias("phone_count_minus_car_count"),
    )
    .orderBy("track_id")
)

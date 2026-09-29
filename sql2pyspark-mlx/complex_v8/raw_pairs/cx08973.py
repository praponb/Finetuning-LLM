from pyspark.sql import functions as F

car_by_track = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("car_count"),
        F.round(F.avg("ms_played"), 2).alias("car_avg_ms_played"),
    )
)

phone_by_track = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.sum("ms_played").alias("phone_total_ms_played"),
    )
)

result = (
    car_by_track.alias("a")
    .join(phone_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.car_count"), F.lit(0)).alias("car_count"),
        F.coalesce(F.col("a.car_avg_ms_played"), F.lit(0)).alias("car_avg_ms_played"),
        F.coalesce(F.col("b.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("b.phone_total_ms_played"),
        (F.coalesce(F.col("a.car_count"), F.lit(0)) - F.coalesce(F.col("b.phone_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("track_id")
)

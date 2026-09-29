from pyspark.sql import functions as F

speaker_by_listener = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("speaker_count"),
        F.round(F.avg("ms_played"), 2).alias("speaker_avg_ms_played"),
    )
)

car_by_listener = (
    df_plays
    .filter(F.col("device") == "car")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("car_count"),
        F.round(F.avg("ms_played"), 2).alias("car_avg_ms_played"),
    )
)

result = (
    speaker_by_listener.alias("a")
    .join(car_by_listener.alias("b"), F.col("a.listener_id") == F.col("b.listener_id"), "full")
    .select(
        F.coalesce(F.col("a.listener_id"), F.col("b.listener_id")).alias("listener_id"),
        F.coalesce(F.col("a.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.col("a.speaker_avg_ms_played"),
        F.coalesce(F.col("b.car_count"), F.lit(0)).alias("car_count"),
        F.coalesce(F.col("b.car_avg_ms_played"), F.lit(0)).alias("car_avg_ms_played"),
        (F.coalesce(F.col("a.speaker_count"), F.lit(0)) - F.coalesce(F.col("b.car_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("listener_id")
)

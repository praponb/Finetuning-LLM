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

speaker_by_track = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("speaker_count"),
        F.round(F.avg("ms_played"), 2).alias("speaker_avg_ms_played"),
    )
)

result = (
    car_by_track.alias("a")
    .join(speaker_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.car_count"), F.lit(0)).alias("car_count"),
        F.col("a.car_avg_ms_played"),
        F.coalesce(F.col("b.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.col("b.speaker_avg_ms_played"),
        (F.coalesce(F.col("a.car_count"), F.lit(0)) - F.coalesce(F.col("b.speaker_count"), F.lit(0))).alias("car_count_minus_speaker_count"),
    )
    .orderBy("track_id")
)

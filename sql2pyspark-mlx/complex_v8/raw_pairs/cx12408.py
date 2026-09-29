from pyspark.sql import functions as F

phone_by_track = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.round(F.avg("ms_played"), 2).alias("phone_avg_ms_played"),
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
    phone_by_track.alias("a")
    .join(speaker_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("a.phone_avg_ms_played"),
        F.coalesce(F.col("b.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.col("b.speaker_avg_ms_played"),
        (F.coalesce(F.col("a.phone_count"), F.lit(0)) - F.coalesce(F.col("b.speaker_count"), F.lit(0))).alias("phone_count_minus_speaker_count"),
    )
    .orderBy("track_id")
)

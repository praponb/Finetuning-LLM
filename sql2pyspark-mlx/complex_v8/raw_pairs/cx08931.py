from pyspark.sql import functions as F

speaker_by_track = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("speaker_count"),
        F.sum("ms_played").alias("speaker_total_ms_played"),
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
    speaker_by_track.alias("a")
    .join(phone_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.coalesce(F.col("a.speaker_total_ms_played"), F.lit(0)).alias("speaker_total_ms_played"),
        F.coalesce(F.col("b.phone_count"), F.lit(0)).alias("phone_count"),
        F.coalesce(F.col("b.phone_total_ms_played"), F.lit(0)).alias("phone_total_ms_played"),
        (F.coalesce(F.col("a.speaker_count"), F.lit(0)) - F.coalesce(F.col("b.phone_count"), F.lit(0))).alias("speaker_count_minus_phone_count"),
    )
    .orderBy("track_id")
)

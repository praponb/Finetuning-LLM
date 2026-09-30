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

desktop_by_track = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("desktop_count"),
        F.round(F.avg("ms_played"), 2).alias("desktop_avg_ms_played"),
    )
)

result = (
    speaker_by_track.alias("a")
    .join(desktop_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.coalesce(F.col("a.speaker_total_ms_played"), F.lit(0)).alias("speaker_total_ms_played"),
        F.coalesce(F.col("b.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.coalesce(F.col("b.desktop_avg_ms_played"), F.lit(0)).alias("desktop_avg_ms_played"),
        (F.coalesce(F.col("a.speaker_count"), F.lit(0)) - F.coalesce(F.col("b.desktop_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("track_id")
)

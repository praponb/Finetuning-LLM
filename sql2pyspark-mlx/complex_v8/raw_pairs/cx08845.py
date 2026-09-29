from pyspark.sql import functions as F

desktop_by_track = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

speaker_by_track = (
    df_plays
    .filter(F.col("device") == "speaker")
    .groupBy("track_id")
    .agg(
        F.count("*").alias("speaker_count"),
        F.sum("ms_played").alias("speaker_total_ms_played"),
    )
)

result = (
    desktop_by_track.alias("a")
    .join(speaker_by_track.alias("b"), F.col("a.track_id") == F.col("b.track_id"), "full")
    .select(
        F.coalesce(F.col("a.track_id"), F.col("b.track_id")).alias("track_id"),
        F.coalesce(F.col("a.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.col("a.desktop_total_ms_played"),
        F.coalesce(F.col("b.speaker_count"), F.lit(0)).alias("speaker_count"),
        F.coalesce(F.col("b.speaker_total_ms_played"), F.lit(0)).alias("speaker_total_ms_played"),
        (F.coalesce(F.col("a.desktop_count"), F.lit(0)) - F.coalesce(F.col("b.speaker_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("track_id")
)

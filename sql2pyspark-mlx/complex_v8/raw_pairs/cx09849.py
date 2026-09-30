from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played_2"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        "p.avg_play_ms_played_2",
        "p.last_played_at",
    )
)

grouped = (
    with_counts
    .filter(F.col("avg_play_ms_played") > 143250.0)
    .groupBy(
        "artist_id",
        F.when(F.col("avg_play_ms_played_2") >= 242000.0, "large").when(F.col("avg_play_ms_played_2") >= 163000.0, "medium").otherwise("small").alias("avg_play_ms_played_2_tier"),
        F.date_trunc("month", "last_played_at").alias("last_played_at_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("title").isNotNull(), 1)) / F.count("*"), 1).alias("pct_title_is_not_null"),
        F.round(F.sum("avg_play_ms_played") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("avg_play_ms_played_to_bpm_ratio"),
        F.sum(F.when(F.col("is_explicit"), F.col("duration_sec")).otherwise(0)).alias("duration_sec_is_explicit_true"),
        F.max("last_played_at").alias("last_last_played_at"),
        F.round(F.sum("bpm") / F.nullif(F.sum("avg_play_ms_played_2"), F.lit(0)), 3).alias("bpm_to_avg_play_ms_played_2_ratio"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

with_stats = (
    grouped.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    with_stats
    .filter(F.col("last_last_played_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "artist_id",
        "avg_play_ms_played_2_tier",
        "avg_play_ms_played_to_bpm_ratio",
        "duration_sec_is_explicit_true",
        "last_last_played_at",
    )
    .orderBy(F.col("artist_id").asc_nulls_last())
)

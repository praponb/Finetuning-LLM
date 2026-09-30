from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.col("release_date").between(F.lit("2024-04-21").cast("date"), F.lit("2025-08-07").cast("date"))))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

with_counts = (
    base.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.first_track_release_date",
    )
)

result = (
    with_counts
    .cube("track_count", "track_is_explicit_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("bpm").alias("total_bpm"),
        F.min("track_is_explicit_count").alias("min_track_is_explicit_count"),
    )
    .orderBy(
        F.col("track_count").asc_nulls_last(),
        F.col("track_is_explicit_count").asc_nulls_last(),
    )
)

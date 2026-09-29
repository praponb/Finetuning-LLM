from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.when(F.col("duration_sec") >= 357, "A").when(F.col("duration_sec") >= 184, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count"),
    )
)

summarized_rows = (
    scoped_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count_2"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count_2"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
        F.coalesce(F.col("t.track_is_explicit_count_2"), F.lit(0)).alias("track_is_explicit_count_2"),
        "t.first_track_release_date",
    )
)

enriched = (
    summarized_rows_2
    .filter(~F.col("duration_sec_tier").isin("A", "B"))
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_tier",
        "track_count",
        "track_is_explicit_count_2",
        "first_track_release_date",
        F.least(F.col("duration_sec"), F.col("track_count_2")).alias("least_duration_sec_track_count_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "is_explicit",
        "duration_sec_tier",
        "track_count",
        "track_is_explicit_count_2",
        "first_track_release_date",
    )
    .orderBy(F.col("track_count").asc())
)

from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id") >= 10) & (F.col("bpm") <= 143))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"), F.count("*").alias("track_count_2"))
)

profiled = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

per_label = (
    profiled
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "bpm_per_duration_sec",
        "track_count",
        "track_count_2",
        F.explode("moods").alias("mood"),
    )
)

result = (
    per_label
    .groupingSets(
        [["track_count_2", "mood", "track_count"], ["track_count_2"], ["mood"]],
        "track_count_2", "mood", "track_count",
    )
    .agg(
        F.sum("track_count_2").alias("total_track_count_2"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("track_count_2").asc_nulls_last(),
        F.col("mood").asc_nulls_last(),
        F.col("track_count").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

without_matches = (
    scoped_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

artist_id_stats = (
    without_matches
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

outliers = (
    without_matches.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

result = (
    outliers
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "median_bpm",
        "bpm_minus_median",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

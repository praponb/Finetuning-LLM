from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 6) & (F.col("release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-03-29").cast("date"))))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

tagged = (
    cleaned
    .filter(F.col("artist_id") <= 13)
    .select(
        "*",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_details_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "inner")
    .filter(F.col("a.genre") == "HipHop")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.max("release_date").alias("last_track_release_date_2"),
    )
)

with_counts = (
    with_details_artist.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        "t.last_track_release_date_2",
    )
)

result = (
    with_counts
    .groupingSets(
        [["track_is_explicit_count", "artist_id", "artist_country"], ["track_is_explicit_count"], ["artist_id"]],
        "track_is_explicit_count", "artist_id", "artist_country",
    )
    .agg(
        F.grouping("track_is_explicit_count").alias("is_total_track_is_explicit_count"),
        F.datediff(F.max("last_track_release_date"), F.min("last_track_release_date")).alias("last_track_release_date_span_days"),
        F.sum("artist_debut_year").alias("total_artist_debut_year"),
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
    )
    .orderBy(
        F.col("track_is_explicit_count").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)

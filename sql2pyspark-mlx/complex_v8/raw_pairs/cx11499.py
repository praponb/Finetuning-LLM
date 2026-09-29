from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 155, "large").when(F.col("bpm") >= 96, "medium").otherwise("small").alias("bpm_tier"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_activity = (
    scoped_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 156), "left_semi")
)

joined_artist = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.bpm_tier",
        "s.bpm_filled",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

enriched = (
    joined_artist
    .select(
        "id",
        "duration_sec",
        "bpm",
        "bpm_tier",
        "artist_genre",
        "artist_debut_year",
        "artist_is_verified",
        F.concat(F.col("bpm_tier"), F.lit("#"), F.col("artist_debut_year").cast("string")).alias("bpm_tier_ref"),
    )
)

rollup_base = (
    enriched
    .filter(F.col("bpm") <= 147)
    .groupBy("artist_is_verified", "artist_genre")
    .agg(
        F.round(F.sum("bpm") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("bpm_to_duration_sec_ratio"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("bpm_to_duration_sec_ratio").alias("overall_avg_bpm_to_duration_sec_ratio")))
    .filter(F.col("bpm_to_duration_sec_ratio") > F.col("overall_avg_bpm_to_duration_sec_ratio"))
    .drop("overall_avg_bpm_to_duration_sec_ratio")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.max("bpm_to_duration_sec_ratio").alias("overall_max_bpm_to_duration_sec_ratio")))
    .filter(F.col("bpm_to_duration_sec_ratio") < F.col("overall_max_bpm_to_duration_sec_ratio"))
    .drop("overall_max_bpm_to_duration_sec_ratio")
)

result = (
    without_matches
    .groupBy("distinct_artist_genres", "artist_is_verified")
    .pivot("artist_genre", ["Jazz", "Classical", "Indie"])
    .agg(F.sum("bpm_to_duration_sec_ratio"))
    .orderBy("distinct_artist_genres", "artist_is_verified")
)

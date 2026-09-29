from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) & (F.col("bpm").between(75, 166)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.when(F.col("bpm") >= 147, "HIGH").when(F.col("bpm") >= 89, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
    )
)

with_details = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

with_details_2 = (
    with_details.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.country").isin("BR", "KR")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.duration_sec_per_bpm",
        "s.bpm_tier",
        "s.has_sad",
        "s.artist_is_verified",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

tagged = (
    with_details_2
    .filter(F.col("duration_sec_per_bpm") >= 1.0)
    .select(
        "*",
        F.substring(F.trim(F.col("bpm_tier")), 1, 3).alias("bpm_tier_prefix"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("duration_sec").alias("min_track_duration_sec"),
        F.count("*").alias("track_count"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.min_track_duration_sec",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "duration_sec_per_bpm",
        "artist_is_verified",
        "artist_is_verified_2",
        "artist_debut_year",
        "artist_country",
        "bpm_tier_prefix",
        "min_track_duration_sec",
        "track_count",
        F.round(F.col("duration_sec") / F.nullif(F.col("track_count"), F.lit(0)), 3).alias("duration_sec_per_track_count"),
        F.greatest(F.col("track_count"), F.col("min_track_duration_sec")).alias("greatest_track_count_min_track_duration_sec"),
    )
    .orderBy(F.col("artist_debut_year").desc_nulls_first(), "id")
    .limit(10)
)

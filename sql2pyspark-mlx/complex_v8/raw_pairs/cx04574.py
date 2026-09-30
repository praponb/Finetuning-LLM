from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id") <= 13) & (F.col("bpm") < 143))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

screened = (
    cleaned_tracks
    .crossJoin(cleaned_tracks.agg(F.max("bpm_per_duration_sec").alias("overall_max_bpm_per_duration_sec")))
    .filter(F.col("bpm_per_duration_sec") < F.col("overall_max_bpm_per_duration_sec"))
    .drop("overall_max_bpm_per_duration_sec")
)

per_label = (
    screened
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm_per_duration_sec",
        "days_since_release_date",
        F.explode("moods").alias("mood"),
    )
)

filtered = (
    per_label.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") < 2006), "left_anti")
)

enriched_artist = (
    filtered_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm_per_duration_sec",
        "s.days_since_release_date",
        "s.mood",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm_per_duration_sec",
        "days_since_release_date",
        "artist_is_verified",
        "artist_name",
    )
    .orderBy(F.col("artist_id").desc(), F.col("bpm_per_duration_sec").desc())
)

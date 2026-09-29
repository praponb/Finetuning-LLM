from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-08-28").cast("date"))) & (F.col("artist_id") >= 7))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

combined_details_artist = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

scored = (
    combined_details_artist
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 163, "HIGH").when(F.col("bpm") >= 100, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        (F.col("artist_debut_year") - F.col("duration_sec")).alias("artist_debut_year_minus_duration_sec"),
    )
)

result = (
    scored
    .select("id", "duration_sec", "artist_debut_year_minus_duration_sec")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "artist_debut_year_minus_duration_sec"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)

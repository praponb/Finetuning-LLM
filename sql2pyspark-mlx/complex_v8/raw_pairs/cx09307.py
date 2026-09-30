from pyspark.sql import functions as F

prepared_artists = (
    df_artists
    .filter((F.length(F.trim(F.col("artist_name"))) >= 7) & (F.col("is_verified")))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
    )
)

without_matches = (
    prepared_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.release_date").isNotNull()), "left_anti")
)

line_level = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.duration_sec") < 312), "inner")
    .select(
        "s.id",
        "s.debut_year",
        "s.is_verified",
        "s.debut_year_bucket",
        F.col("t.id").alias("track_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

filtered = (
    line_level.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.array_contains(F.col("t.moods"), "workout")), "left_anti")
    .filter(F.col("is_verified"))
)

result = (
    filtered
    .filter(F.col("track_bpm").between(75, 142))
    .select(
        "id",
        "debut_year",
        "is_verified",
        "debut_year_bucket",
        "track_id",
        "track_duration_sec",
        (F.floor(F.col("track_bpm") / 25) * 25).alias("track_bpm_bucket"),
    )
    .orderBy(F.col("debut_year").asc(), "id", "track_id")
    .limit(5)
)

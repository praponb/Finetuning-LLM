from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 14)
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
    )
)

with_parent_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") <= 2006), "inner")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"), "a.artist_name")
)

result = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "least_duration_sec_bpm",
        "artist_debut_year",
        "artist_name",
    )
    .orderBy(F.col("artist_debut_year").desc(), F.col("id").asc_nulls_last())
)

from pyspark.sql import functions as F

recent_tracks = df_tracks

with_activity = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") >= 1996) & (F.col("a.country") == "BR"), "left_semi")
)

joined_artist = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.country") == "US")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

labeled = (
    joined_artist
    .filter(F.col("artist_genre") == "Classical")
    .select(
        "*",
        F.abs(F.col("duration_sec") - 344).alias("duration_sec_distance"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

result = (
    labeled
    .filter(F.col("artist_debut_year") < 2018)
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "artist_debut_year",
        "duration_sec_distance",
        "duration_sec_filled",
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
)

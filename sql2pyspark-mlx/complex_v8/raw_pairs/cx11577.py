from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) & (F.col("bpm") <= 142))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.size(F.col("moods")).alias("moods_count"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

combined_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods_count",
        "s.title_len",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

without_matches = (
    combined_details_artist.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.is_explicit")), "left_semi")
)

combined_details = (
    without_matches.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.moods_count",
        "s.title_len",
        "s.artist_genre",
        "s.artist_debut_year",
        "s.artist_is_verified",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

result = (
    combined_details
    .groupBy("moods_count", "artist_genre_2")
    .pivot("artist_genre", ["Indie", "Pop", "Classical", "HipHop"])
    .agg(F.min("duration_sec"))
    .orderBy("moods_count", "artist_genre_2")
)

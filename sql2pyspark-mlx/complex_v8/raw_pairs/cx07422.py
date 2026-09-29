from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

joined_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "HipHop")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
    )
)

annotated = (
    joined_artist
    .filter(F.col("duration_sec") <= 365)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "artist_genre",
        F.length(F.trim(F.col("artist_genre"))).alias("artist_genre_len"),
    )
)

result = (
    annotated
    .groupBy("artist_genre_len")
    .pivot("artist_genre", ["HipHop", "Pop", "Classical", "Indie"])
    .agg(F.sum("bpm"))
    .orderBy("artist_genre_len")
)

from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

labeled = (
    with_details
    .select(
        "*",
        (F.col("bpm") - F.col("artist_debut_year")).alias("bpm_minus_artist_debut_year"),
        F.concat(F.col("artist_genre"), F.lit("#"), F.col("artist_debut_year").cast("string")).alias("artist_genre_ref"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

result = (
    labeled
    .groupBy("is_explicit", "artist_id")
    .pivot("artist_genre", ["Pop", "Indie", "HipHop"])
    .agg(F.count("duration_sec_per_bpm").alias("count_v"), F.max("duration_sec_per_bpm").alias("max_v"))
    .orderBy("is_explicit", "artist_id")
)

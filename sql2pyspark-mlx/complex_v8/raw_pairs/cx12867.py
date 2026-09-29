from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("C%")) & (F.col("release_date") >= F.lit("2024-09-26").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

flattened = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "artist_genre",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

result = (
    flattened
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "mood_pos",
        "mood",
        F.when(F.col("mood_pos") >= 2, "large").when(F.col("mood_pos") >= 1, "medium").otherwise("small").alias("mood_pos_tier"),
    )
    .orderBy(F.col("mood_pos_tier").desc(), F.col("mood_pos").asc_nulls_last())
)

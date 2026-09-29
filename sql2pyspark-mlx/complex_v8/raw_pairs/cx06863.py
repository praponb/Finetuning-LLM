from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.col("artist_id") <= 3) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

combined_details_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details_artist
    .groupBy("release_date", "release_date_month")
    .pivot("artist_country", ["KR", "BR"])
    .agg(F.sum("bpm").alias("total"), F.max("bpm").alias("max_v"))
)

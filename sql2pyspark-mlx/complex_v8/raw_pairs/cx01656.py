from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((F.col("artist_id") >= 8) & (F.upper(F.trim(F.col("title"))).like("G%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

track_side = cleaned.select(F.col("bpm").alias("ref_id"), "title")

artist_side = df_artists.select(F.col("id").alias("ref_id"), F.col("genre").alias("title"))

result = (
    track_side
    .unionByName(artist_side)
    .distinct()
)

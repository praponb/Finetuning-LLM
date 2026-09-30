from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.col("artist_id") <= 12) & (F.col("bpm") >= 96))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

track_rows = df_tracks.select("artist_id", F.col("title").alias("label"))

track_side = df_tracks.select("artist_id", "title")

result = track_rows.exceptAll(track_side)

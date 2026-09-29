from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-09-26").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

track_side = df_tracks.select(F.col("id").alias("ref_id"), F.col("title").alias("category"))

play_side = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")) & (F.col("ms_played") < 223262))
    .select("track_id", "source")
)

result = track_side.exceptAll(play_side)

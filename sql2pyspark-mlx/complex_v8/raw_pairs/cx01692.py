from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "sad")) & (F.lower(F.col("title")).like("%lo%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 142).alias("bpm_distance"),
    )
)

track_side = valid_tracks.select("duration_sec", F.col("title").alias("label"))

listener_side = df_listeners.select("id", "country")

result = track_side.subtract(listener_side)

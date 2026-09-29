from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
    )
)

track_side = valid_tracks.select(F.col("id").alias("qty"), F.col("title").alias("label"))

listener_rows = df_listeners.select("birth_year", "country")

result = track_side.intersectAll(listener_rows)

from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

track_rows = (
    cleaned_tracks
    .filter(F.col("title").isNotNull())
    .select(F.col("id").alias("entity_id"), "release_date")
)

listener_side = df_listeners.select("birth_year", "signup_date")

track_side = df_tracks.select("bpm", "release_date")

result = (
    track_rows
    .union(listener_side)
    .union(track_side)
    .orderBy(F.col("entity_id").desc())
)

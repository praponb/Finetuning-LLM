from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(F.col("bpm") <= 163)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

track_rows = (
    filtered
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        F.col("moods_text").alias("category"),
        F.col("release_date").alias("day"),
    )
)

listener_rows = (
    df_listeners
    .filter(F.col("country") == "KR")
    .select(
        F.col("birth_year").alias("id"),
        F.col("username").alias("category"),
        F.col("signup_date").alias("day"),
    )
)

result = track_rows.unionByName(listener_rows)

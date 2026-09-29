from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "focus"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

track_rows = filtered.select("title_prefix")

listener_side = df_listeners.select("country")

result = (
    track_rows
    .intersect(listener_side)
    .orderBy(F.col("title_prefix").desc())
)

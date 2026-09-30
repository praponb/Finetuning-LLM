from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

track_rows = df_tracks.select("artist_id", "title")

track_rows_2 = df_tracks.select("bpm", "title")

result = track_rows.intersect(track_rows_2)

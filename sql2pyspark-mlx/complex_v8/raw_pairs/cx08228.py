from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.col("bpm") < 142)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

track_rows = cleaned_tracks.select(F.lit("track").alias("origin"), "duration_sec", "release_date")

listener_rows = (
    df_listeners
    .filter((F.col("plan").isNotNull()) & (F.length(F.trim(F.col("username"))) >= 4))
    .select(F.lit("listener").alias("origin"), "birth_year", "signup_date")
)

result = track_rows.union(listener_rows)

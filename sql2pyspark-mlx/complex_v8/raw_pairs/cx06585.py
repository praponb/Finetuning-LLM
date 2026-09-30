from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

track_side = (
    filtered_tracks
    .select(
        "title",
        F.col("release_date").alias("day"),
        F.lit("track").alias("source"),
    )
)

track_side_2 = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "title",
        F.col("release_date").alias("day"),
        F.lit("track_2").alias("source"),
    )
)

result = track_side.unionByName(track_side_2)

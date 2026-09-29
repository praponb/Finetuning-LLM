from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm").between(89, 142)) & (F.col("release_date") < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

with_activity = (
    prepared_tracks
    .crossJoin(prepared_tracks.agg(F.max("bpm").alias("overall_max_bpm")))
    .filter(F.col("bpm") < F.col("overall_max_bpm"))
    .drop("overall_max_bpm")
)

exploded = (
    with_activity
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.explode("moods").alias("mood"),
    )
)

filtered = (
    exploded.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date") < F.lit("2025-08-07").cast("date")) & (F.length(F.trim(F.col("t.title"))) >= 6), "left_semi")
    .filter(F.col("mood") == "workout")
)

result = (
    filtered
    .groupBy("artist_id", "release_date")
    .pivot("mood", ["chill", "sad", "workout"])
    .agg(F.avg("duration_sec").alias("avg_v"), F.max("duration_sec").alias("max_v"))
    .orderBy("artist_id", "release_date")
)

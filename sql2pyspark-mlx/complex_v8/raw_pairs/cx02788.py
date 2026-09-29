from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("bpm") < 127) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
    )
)

per_label = (
    valid
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        F.explode("moods").alias("mood"),
    )
)

result = (
    per_label
    .groupBy("artist_id")
    .pivot("mood", ["party", "chill", "sad"])
    .agg(F.max("bpm").alias("max_v"), F.sum("bpm").alias("sum_v"))
)

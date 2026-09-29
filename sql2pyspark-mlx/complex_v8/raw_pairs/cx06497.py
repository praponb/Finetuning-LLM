from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .select(
        F.lit("artist").alias("source"),
        "id",
        F.col("debut_year").alias("key_id"),
    )
)

track_side = (
    df_tracks
    .select(
        F.lit("track").alias("source"),
        F.col("bpm").alias("id"),
        F.col("artist_id").alias("key_id"),
    )
)

result = (
    artist_rows
    .unionByName(track_side)
    .distinct()
    .orderBy("source")
)

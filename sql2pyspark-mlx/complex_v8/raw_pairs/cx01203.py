from pyspark.sql import functions as F

artist_side = (
    df_artists
    .select(
        F.col("debut_year").alias("key_id"),
        F.col("country").alias("category"),
        F.lit("artist").alias("src"),
    )
)

track_side = (
    df_tracks
    .filter(F.col("duration_sec") > 209)
    .select("artist_id", "title", F.lit("track").alias("src"))
)

result = (
    artist_side
    .union(track_side)
    .orderBy("key_id", "category")
)

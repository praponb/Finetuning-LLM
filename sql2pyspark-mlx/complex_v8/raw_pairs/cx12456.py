from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .select(
        F.lit("listener").alias("src"),
        F.col("plan").alias("label"),
        F.col("id").alias("ref_id"),
    )
)

artist_rows = (
    df_artists
    .select(
        F.lit("artist").alias("src"),
        F.col("country").alias("label"),
        F.col("id").alias("ref_id"),
    )
)

track_rows = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.lower(F.col("title")).like("%dr%")))
    .select(
        F.lit("track").alias("src"),
        F.col("title").alias("label"),
        F.col("id").alias("ref_id"),
    )
)

result = (
    listener_side
    .unionByName(artist_rows)
    .unionByName(track_rows)
)

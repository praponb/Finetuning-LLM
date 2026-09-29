from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.col("birth_year").alias("key_id"),
        F.col("signup_date").alias("event_date"),
        F.lit("listener").alias("origin"),
    )
)

track_side = (
    df_tracks
    .select(
        F.col("bpm").alias("key_id"),
        F.col("release_date").alias("event_date"),
        F.lit("track").alias("origin"),
    )
)

result = listener_rows.unionByName(track_side)

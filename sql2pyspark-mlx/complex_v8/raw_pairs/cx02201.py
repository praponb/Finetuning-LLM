from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .select(
        F.col("title").alias("label"),
        F.col("release_date").alias("day"),
        F.col("duration_sec").alias("qty"),
    )
)

track_side = (
    df_tracks
    .select(
        F.col("title").alias("label"),
        F.col("release_date").alias("day"),
        F.col("id").alias("qty"),
    )
)

result = (
    track_rows
    .unionByName(track_side)
    .orderBy("qty")
)

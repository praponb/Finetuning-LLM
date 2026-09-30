from pyspark.sql import functions as F

track_side = (
    df_tracks
    .select(
        F.col("title").alias("name"),
        "id",
        "release_date",
        F.lit("track").alias("origin"),
    )
)

track_side_2 = (
    df_tracks
    .select(
        "title",
        "artist_id",
        "release_date",
        F.lit("track_2").alias("origin"),
    )
)

result = (
    track_side
    .union(track_side_2)
    .orderBy("name")
)

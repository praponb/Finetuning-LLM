from pyspark.sql import functions as F

track_rows = df_tracks.select(F.col("title").alias("name"))

track_side = df_tracks.select("title")

result = (
    track_rows
    .subtract(track_side)
    .orderBy(F.col("name").desc())
)

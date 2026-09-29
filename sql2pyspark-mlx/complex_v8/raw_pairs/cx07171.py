from pyspark.sql import functions as F

listener_rows = df_listeners.select("birth_year", F.col("username").alias("category"))

track_rows = df_tracks.select("artist_id", "title")

result = (
    listener_rows
    .union(track_rows)
    .distinct()
    .orderBy(F.col("birth_year").desc(), "category")
)

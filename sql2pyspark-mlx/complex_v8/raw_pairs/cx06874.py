from pyspark.sql import functions as F

listener_rows = df_listeners.select(F.col("plan").alias("name"), F.col("signup_date").alias("day"))

track_side = df_tracks.select("title", "release_date")

result = (
    listener_rows
    .union(track_side)
    .orderBy("name", "day")
)

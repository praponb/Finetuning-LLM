from pyspark.sql import functions as F

listener_side = df_listeners.select("birth_year")

play_rows = df_plays.select(F.col("track_id").alias("birth_year"))

result = (
    listener_side
    .intersect(play_rows)
    .orderBy("birth_year")
)

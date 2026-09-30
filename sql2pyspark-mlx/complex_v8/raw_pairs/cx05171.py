from pyspark.sql import functions as F

listener_side = df_listeners.select(F.col("birth_year").alias("ref_id"), "username")

play_side = df_plays.select(F.col("track_id").alias("ref_id"), F.col("device").alias("username"))

result = (
    listener_side
    .intersectAll(play_side)
    .orderBy("username", "ref_id")
)

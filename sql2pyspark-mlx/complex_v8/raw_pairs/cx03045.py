from pyspark.sql import functions as F

play_side = df_plays.select("track_id", F.col("source").alias("name"))

listener_side = (
    df_listeners
    .filter(F.col("plan") == "free")
    .select("birth_year", "username")
)

result = play_side.intersect(listener_side)

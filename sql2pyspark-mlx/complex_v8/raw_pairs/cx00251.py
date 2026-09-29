from pyspark.sql import functions as F

play_side = df_plays.select("source", F.col("device").alias("name"))

listener_rows = df_listeners.select("plan", "username")

result = (
    play_side
    .union(listener_rows)
    .distinct()
    .orderBy(F.col("source").desc(), "name")
)

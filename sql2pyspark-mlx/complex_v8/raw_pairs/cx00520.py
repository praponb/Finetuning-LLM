from pyspark.sql import functions as F

listener_side = df_listeners.select(F.col("plan").alias("name"))

play_side = (
    df_plays
    .filter(F.col("source") == "search")
    .select(F.col("device").alias("name"))
)

result = (
    listener_side
    .intersect(play_side)
    .orderBy(F.col("name").desc())
)

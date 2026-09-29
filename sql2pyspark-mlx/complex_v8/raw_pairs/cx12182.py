from pyspark.sql import functions as F

play_side = df_plays.select(F.col("device").alias("name"))

listener_side = (
    df_listeners
    .filter((F.col("username").isNotNull()) & (F.col("plan") == "family"))
    .select("username")
)

result = (
    play_side
    .intersectAll(listener_side)
    .orderBy("name")
)

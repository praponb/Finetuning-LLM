from pyspark.sql import functions as F

play_side = df_plays.select("id", F.col("source").alias("label"))

listener_side = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "jazz"))
    .select("id", "plan")
)

unioned = (
    play_side
    .union(listener_side)
    .distinct()
)

result = (
    unioned
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("label")
)

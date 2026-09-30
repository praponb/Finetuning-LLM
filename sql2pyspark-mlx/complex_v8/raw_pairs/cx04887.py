from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("listener_id") >= 17))
    .select(F.col("id").alias("qty"), F.col("source").alias("name"))
)

listener_side = df_listeners.select("birth_year", "country")

result = (
    play_side
    .subtract(listener_side)
    .orderBy("name", "qty")
)

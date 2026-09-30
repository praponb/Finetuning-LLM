from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")) & (F.col("ms_played") > 160264))
    .select(F.col("source").alias("category"), "device")
)

play_side_2 = df_plays.select("source", "device")

listener_rows = df_listeners.select("country", "username")

result = (
    play_side
    .union(play_side_2)
    .union(listener_rows)
    .orderBy("device", "category")
)

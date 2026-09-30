from pyspark.sql import functions as F

play_rows = df_plays.select(F.col("source").alias("name"), "played_at")

play_side = df_plays.select("source", "played_at")

play_side_2 = df_plays.select("device", "played_at")

result = (
    play_rows
    .union(play_side)
    .union(play_side_2)
)

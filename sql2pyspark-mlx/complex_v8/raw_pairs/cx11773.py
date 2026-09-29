from pyspark.sql import functions as F

play_rows = df_plays.select("track_id", "played_at", F.lit("play").alias("src"))

play_rows_2 = df_plays.select("id", "played_at", F.lit("play_2").alias("src"))

play_side = df_plays.select("track_id", "played_at", F.lit("play_3").alias("src"))

result = (
    play_rows
    .union(play_rows_2)
    .union(play_side)
    .orderBy(F.col("track_id").desc())
)

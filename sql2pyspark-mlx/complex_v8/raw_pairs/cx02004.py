from pyspark.sql import functions as F

play_rows = (
    df_plays
    .select(
        F.lit("play").alias("src"),
        F.col("listener_id").alias("key_id"),
        F.col("played_at").alias("happened_at"),
    )
)

play_side = df_plays.select(F.lit("play_2").alias("src"), "ms_played", "played_at")

result = play_rows.union(play_side)

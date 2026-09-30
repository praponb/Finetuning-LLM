from pyspark.sql import functions as F

play_rows = df_plays.select("id", "track_id", F.col("played_at").alias("happened_at"))

play_side = (
    df_plays
    .select(
        F.col("listener_id").alias("id"),
        "track_id",
        F.col("played_at").alias("happened_at"),
    )
)

result = (
    play_rows
    .unionByName(play_side)
    .orderBy("track_id")
)

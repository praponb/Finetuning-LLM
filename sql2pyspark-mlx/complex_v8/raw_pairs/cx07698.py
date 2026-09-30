from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter(F.col("listener_id") <= 19)
    .select(
        F.col("listener_id").alias("ref_id"),
        F.col("device").alias("name"),
        F.col("played_at").alias("happened_at"),
        F.lit("play").alias("origin"),
    )
)

play_rows_2 = (
    df_plays
    .select(
        F.col("track_id").alias("ref_id"),
        F.col("device").alias("name"),
        F.col("played_at").alias("happened_at"),
        F.lit("play_2").alias("origin"),
    )
)

result = (
    play_rows
    .unionByName(play_rows_2)
    .orderBy("origin", "name")
)

from pyspark.sql import functions as F

play_side = df_plays.select("device", "played_at", F.lit("play").alias("src"))

play_rows = (
    df_plays
    .select(
        F.col("source").alias("device"),
        "played_at",
        F.lit("play_2").alias("src"),
    )
)

play_side_2 = (
    df_plays
    .filter(F.col("listener_id") <= 4)
    .select("device", "played_at", F.lit("play_3").alias("src"))
)

result = (
    play_side
    .unionByName(play_rows)
    .unionByName(play_side_2)
    .orderBy("src")
)

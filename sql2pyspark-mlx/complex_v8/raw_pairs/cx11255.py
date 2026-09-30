from pyspark.sql import functions as F

play_rows = df_plays.select("source", "device", F.lit("play").alias("src"))

play_side = (
    df_plays
    .select(
        F.col("device").alias("source"),
        F.col("source").alias("device"),
        F.lit("play_2").alias("src"),
    )
)

result = (
    play_rows
    .unionByName(play_side)
    .orderBy(F.col("device").desc())
)

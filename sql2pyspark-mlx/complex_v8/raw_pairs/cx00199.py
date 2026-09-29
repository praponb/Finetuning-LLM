from pyspark.sql import functions as F

play_side = (
    df_plays
    .select(
        F.lit("play").alias("origin"),
        F.col("ms_played").alias("key_id"),
        "played_at",
        F.col("device").alias("category"),
    )
)

play_side_2 = (
    df_plays
    .select(
        F.lit("play_2").alias("origin"),
        "ms_played",
        "played_at",
        "source",
    )
)

result = (
    play_side
    .union(play_side_2)
    .distinct()
)

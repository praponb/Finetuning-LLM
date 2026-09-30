from pyspark.sql import functions as F

play_side = (
    df_plays
    .select(
        "id",
        "source",
        "played_at",
        F.lit("play").alias("origin"),
    )
)

play_side_2 = (
    df_plays
    .filter((F.col("device").isin("car", "desktop", "phone")) & (F.col("track_id") >= 21))
    .select(
        F.col("track_id").alias("id"),
        F.col("device").alias("source"),
        "played_at",
        F.lit("play_2").alias("origin"),
    )
)

result = (
    play_side
    .union(play_side_2)
    .distinct()
)

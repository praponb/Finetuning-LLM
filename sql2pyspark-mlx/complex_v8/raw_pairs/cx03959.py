from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        F.lit("play").alias("source"),
        "device",
        "played_at",
        F.col("listener_id").alias("key_id"),
    )
)

play_rows = (
    df_plays
    .select(
        F.lit("play_2").alias("source"),
        "device",
        "played_at",
        F.col("ms_played").alias("key_id"),
    )
)

result = (
    play_side
    .union(play_rows)
    .distinct()
)

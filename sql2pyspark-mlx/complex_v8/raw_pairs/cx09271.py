from pyspark.sql import functions as F

play_rows = (
    df_plays
    .select(
        F.lit("play").alias("origin"),
        F.col("ms_played").alias("key_id"),
        F.col("played_at").alias("event_time"),
    )
)

play_side = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(F.lit("play_2").alias("origin"), "id", "played_at")
)

result = play_rows.union(play_side)

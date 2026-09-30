from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter(F.col("device") == "desktop")
    .select(
        F.lit("play").alias("record_type"),
        "device",
        F.col("listener_id").alias("key_id"),
        F.col("played_at").alias("happened_at"),
    )
)

play_side_2 = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        F.lit("play_2").alias("record_type"),
        F.col("source").alias("device"),
        F.col("ms_played").alias("key_id"),
        F.col("played_at").alias("happened_at"),
    )
)

result = play_side.unionByName(play_side_2)

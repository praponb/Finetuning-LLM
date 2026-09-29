from pyspark.sql import functions as F

play_rows = (
    df_plays
    .select(
        "id",
        "track_id",
        F.col("played_at").alias("event_time"),
        F.lit("play").alias("source"),
    )
)

play_side = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        F.col("ms_played").alias("track_id"),
        F.col("played_at").alias("event_time"),
        F.lit("play_2").alias("source"),
    )
)

play_rows_2 = (
    df_plays
    .select(
        F.col("track_id").alias("id"),
        F.col("ms_played").alias("track_id"),
        F.col("played_at").alias("event_time"),
        F.lit("play_3").alias("source"),
    )
)

result = (
    play_rows
    .unionByName(play_side)
    .unionByName(play_rows_2)
    .orderBy("id")
)

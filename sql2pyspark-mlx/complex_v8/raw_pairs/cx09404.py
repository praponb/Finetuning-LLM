from pyspark.sql import functions as F

play_rows = (
    df_plays
    .select(
        "device",
        F.col("played_at").alias("event_time"),
        F.col("id").alias("key_id"),
    )
)

play_rows_2 = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(2, 3)) & (F.col("source") != "radio"))
    .select("source", "played_at", "listener_id")
)

play_side = (
    df_plays
    .filter(F.col("ms_played").between(108979, 340114))
    .select("source", "played_at", "track_id")
)

result = (
    play_rows
    .union(play_rows_2)
    .union(play_side)
)

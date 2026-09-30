from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.hour(F.col("played_at")).between(6, 10)))
    .select(F.col("ms_played").alias("key_id"))
)

play_rows = df_plays.select("listener_id")

result = (
    play_side
    .exceptAll(play_rows)
    .orderBy(F.col("key_id").desc())
)

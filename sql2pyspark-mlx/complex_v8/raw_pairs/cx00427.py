from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("ms_played") <= 326250) & (F.col("listener_id") <= 8))
    .select(F.col("listener_id").alias("qty"), "device", "played_at")
)

play_rows_2 = (
    df_plays
    .filter(F.col("device").isin("tv", "desktop"))
    .select(F.col("track_id").alias("qty"), "device", "played_at")
)

result = (
    play_rows
    .union(play_rows_2)
    .orderBy(F.col("device").desc(), "qty")
)

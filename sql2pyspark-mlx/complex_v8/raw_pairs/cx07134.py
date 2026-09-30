from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("track_id").isNotNull()))
    .select("track_id", F.col("source").alias("category"))
)

play_side = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("ms_played").between(72464, 223828)))
    .select("listener_id", "device")
)

result = (
    play_rows
    .subtract(play_side)
    .orderBy(F.col("track_id").desc())
)

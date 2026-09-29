from pyspark.sql import functions as F

base_plays = df_plays.filter(F.col("played_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))

play_side = base_plays.select(F.col("track_id").alias("qty"), F.col("played_at").alias("event_time"))

play_side_2 = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select("track_id", "played_at")
)

result = (
    play_side
    .union(play_side_2)
    .distinct()
)

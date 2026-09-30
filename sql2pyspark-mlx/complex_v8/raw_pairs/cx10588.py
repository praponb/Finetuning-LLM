from pyspark.sql import functions as F

play_rows = df_plays.select(F.col("device").alias("category"))

track_side = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) & (F.col("duration_sec") < 357))
    .select("title")
)

result = play_rows.intersect(track_side)

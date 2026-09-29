from pyspark.sql import functions as F

track_side = df_tracks.select("title")

play_rows = (
    df_plays
    .filter((F.col("device") == "phone") & (~F.col("is_skipped")))
    .select(F.col("device").alias("title"))
)

result = track_side.intersect(play_rows)

from pyspark.sql import functions as F

play_side = df_plays.select(F.col("id").alias("qty"), "source")

track_rows = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-09-26").cast("date")) & (F.lower(F.col("title")).like("%ri%")))
    .select("id", "title")
)

result = (
    play_side
    .exceptAll(track_rows)
    .orderBy("source", "qty")
)

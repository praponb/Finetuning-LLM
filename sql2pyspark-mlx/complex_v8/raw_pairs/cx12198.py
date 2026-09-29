from pyspark.sql import functions as F

artist_side = df_artists.select("artist_name")

play_side = (
    df_plays
    .filter((F.col("listener_id") <= 9) & (F.to_date(F.col("played_at")) < F.lit("2025-02-12").cast("date")))
    .select(F.col("source").alias("artist_name"))
)

result = (
    artist_side
    .subtract(play_side)
    .orderBy("artist_name")
)

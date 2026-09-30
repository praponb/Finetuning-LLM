from pyspark.sql import functions as F

artist_rows = df_artists.select(F.col("id").alias("entity_id"), F.col("artist_name").alias("label"))

play_rows = (
    df_plays
    .filter((F.col("listener_id") >= 11) & (F.hour(F.col("played_at")).between(8, 11)))
    .select("track_id", "source")
)

result = artist_rows.subtract(play_rows)

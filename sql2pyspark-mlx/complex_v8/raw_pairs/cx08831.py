from pyspark.sql import functions as F

artist_side = df_artists.select(F.col("debut_year").alias("entity_id"), "country")

play_side = df_plays.select("ms_played", "source")

result = artist_side.subtract(play_side)

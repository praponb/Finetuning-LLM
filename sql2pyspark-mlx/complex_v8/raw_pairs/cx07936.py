from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter((F.upper(F.trim(F.col("artist_name"))).like("P%")) & (F.col("debut_year") > 1994))
    .select(F.col("id").alias("entity_id"))
)

track_side = df_tracks.select(F.col("artist_id").alias("entity_id"))

result = artist_rows.intersect(track_side)

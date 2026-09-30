from pyspark.sql import functions as F

artist_side = df_artists.select("debut_year")

track_side = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.length(F.trim(F.col("title"))) >= 4))
    .select(F.col("bpm").alias("debut_year"))
)

result = artist_side.subtract(track_side)

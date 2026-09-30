from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 13))
    .select("device")
)

artist_side = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("K%"))
    .select("artist_name")
)

result = play_rows.subtract(artist_side)

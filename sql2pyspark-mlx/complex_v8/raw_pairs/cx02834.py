from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter((~F.col("is_verified")) & (F.upper(F.trim(F.col("artist_name"))).like("S%")))
    .select("id")
)

play_side = (
    df_plays
    .filter((F.col("ms_played") <= 220498) & (~F.col("is_skipped")))
    .select("ms_played")
)

result = artist_side.intersect(play_side)

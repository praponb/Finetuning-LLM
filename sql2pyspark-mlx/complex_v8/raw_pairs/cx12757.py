from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.upper(F.trim(F.col("artist_name"))).like("N%"))
    .select("debut_year")
)

play_rows = (
    df_plays
    .filter((F.col("ms_played").between(26344, 302279)) & (F.col("listener_id").isNotNull()))
    .select("id")
)

result = (
    artist_side
    .intersect(play_rows)
    .orderBy("debut_year")
)

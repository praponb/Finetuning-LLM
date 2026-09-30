from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("listener_id") <= 13) & (F.col("track_id").isNotNull()))
    .select(F.col("ms_played").alias("entity_id"))
)

artist_rows = (
    df_artists
    .filter((F.col("debut_year") < 2015) & (~F.col("is_verified")))
    .select("id")
)

result = (
    play_side
    .subtract(artist_rows)
    .orderBy(F.col("entity_id").desc())
)

from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter(F.col("is_verified"))
    .select("debut_year", F.col("id").alias("key_id"))
)

listener_rows = (
    df_listeners
    .filter((F.col("plan").isNotNull()) & (F.col("birth_year") >= 1969))
    .select(F.col("id").alias("debut_year"), F.col("birth_year").alias("key_id"))
)

play_rows = df_plays.select(F.col("id").alias("debut_year"), F.col("ms_played").alias("key_id"))

result = (
    artist_rows
    .unionByName(listener_rows)
    .unionByName(play_rows)
    .orderBy(F.col("debut_year").desc())
)

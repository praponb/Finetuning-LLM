from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("country") == "JP")
    .select("artist_name", "genre")
)

listener_rows = (
    df_listeners
    .select(F.col("country").alias("artist_name"), F.col("plan").alias("genre"))
)

play_side = (
    df_plays
    .filter((~F.col("is_skipped")) & (~F.col("source").isin("playlist", "radio")))
    .select(F.col("source").alias("artist_name"), F.col("device").alias("genre"))
)

stacked = (
    artist_side
    .union(listener_rows)
    .union(play_side)
)

result = (
    stacked
    .groupBy("artist_name")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("artist_name")
)

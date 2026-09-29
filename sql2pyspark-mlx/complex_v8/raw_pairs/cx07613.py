from pyspark.sql import functions as F

artist_rows = df_artists.select("artist_name", F.col("id").alias("key_id"))

play_rows = (
    df_plays
    .filter(F.col("listener_id") <= 15)
    .select("device", "id")
)

listener_side = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 2)
    .select("username", "id")
)

combined = (
    artist_rows
    .union(play_rows)
    .union(listener_side)
)

result = (
    combined
    .groupBy("artist_name")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("artist_name")
)

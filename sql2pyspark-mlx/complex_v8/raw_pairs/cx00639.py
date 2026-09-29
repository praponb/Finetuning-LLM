from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("source").isin("search", "radio")) & (F.col("is_skipped")))
    .select(F.col("id").alias("qty"), "device")
)

artist_side = df_artists.select("id", "genre")

unioned = play_rows.union(artist_side)

result = (
    unioned
    .groupBy("device")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("device")
)

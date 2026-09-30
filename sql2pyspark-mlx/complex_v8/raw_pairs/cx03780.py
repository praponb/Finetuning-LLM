from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("ms_played") >= 157900)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

play_side = filtered.select(F.col("track_id").alias("key_id"), F.col("source").alias("category"))

artist_side = (
    df_artists
    .filter((F.col("genre").isNotNull()) & (F.col("debut_year") < 2015))
    .select("id", "country")
)

result = play_side.exceptAll(artist_side)

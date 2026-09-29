from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("device") == "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

play_rows = scoped_plays.select("track_id")

artist_side = (
    df_artists
    .filter((F.col("debut_year").between(1990, 2015)) & (F.col("genre") == "Classical"))
    .select("id")
)

result = (
    play_rows
    .intersect(artist_side)
    .orderBy("track_id")
)

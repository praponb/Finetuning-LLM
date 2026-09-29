from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.abs(F.col("ms_played") - 129336).alias("ms_played_distance"),
    )
)

play_rows = (
    cleaned_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(F.col("id").alias("ref_id"))
)

artist_rows = (
    df_artists
    .filter(F.col("country") != "US")
    .select("id")
)

result = play_rows.intersectAll(artist_rows)

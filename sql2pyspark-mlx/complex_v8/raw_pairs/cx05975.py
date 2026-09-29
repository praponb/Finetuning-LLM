from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("ms_played") > 176302) & (F.col("device").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

play_side = recent_plays.select(F.col("track_id").alias("qty"))

track_rows = df_tracks.select(F.col("id").alias("qty"))

result = (
    play_side
    .intersect(track_rows)
    .orderBy("qty")
)

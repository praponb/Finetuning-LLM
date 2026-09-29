from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

result = (
    scoped_plays
    .filter(F.col("source") != "playlist")
    .select(
        "id",
        "track_id",
        "is_skipped",
        "source",
        "ms_played_bucket",
        F.round(F.col("ms_played") / F.nullif(F.col("ms_played_bucket"), F.lit(0)), 3).alias("ms_played_per_ms_played_bucket"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("ms_played_bucket_per_ms_played"),
    )
    .orderBy("track_id", F.col("ms_played_per_ms_played_bucket").desc())
)

from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

result = (
    filtered
    .filter(F.col("ms_played") > 73335)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "ms_played_bucket",
    )
    .orderBy(F.col("listener_id").asc_nulls_last(), F.col("ms_played_bucket").desc())
)

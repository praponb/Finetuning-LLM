from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
    .orderBy(F.col("track_id").desc(), F.col("device").desc_nulls_first(), "id")
    .limit(10)
)

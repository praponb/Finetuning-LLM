from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("track_id") >= 15)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 183186).alias("ms_played_distance"),
    )
)

labeled = (
    valid_plays
    .select(
        "*",
        F.abs(F.col("ms_played") - 183186).alias("ms_played_distance_2"),
        F.when(F.col("device").isin("tv", "phone"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)

result = (
    labeled
    .filter(F.col("ms_played") < 236512)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_distance",
        "ms_played_distance_2",
        "device_group",
    )
    .orderBy("ms_played_distance", F.col("device_group").desc())
)

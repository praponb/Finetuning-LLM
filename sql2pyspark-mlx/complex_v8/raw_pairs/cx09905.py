from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

result = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
    .orderBy(
        F.col("device").desc_nulls_first(),
        F.col("source_device_label").asc_nulls_last(),
    )
)

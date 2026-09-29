from pyspark.sql import functions as F

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

result = (
    base_plays
    .filter(F.col("device_ref").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "device_ref",
    )
    .orderBy(F.col("device_ref").desc(), F.col("id").desc())
)

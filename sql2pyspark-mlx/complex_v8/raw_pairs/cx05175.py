from pyspark.sql import functions as F

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
    )
)

derived = (
    filtered
    .filter(F.col("track_id").isNotNull())
    .withColumn("device_or_unknown", F.coalesce(F.col("device"), F.lit("UNKNOWN")))
)

result = (
    derived
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "device_or_unknown",
    )
    .orderBy(F.col("listener_id").asc())
)

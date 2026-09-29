from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("track_id") <= 28)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

result = (
    filtered
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "device_clean",
    )
)

from pyspark.sql import functions as F

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

tagged = (
    base
    .select(
        "id",
        "device",
        "is_skipped",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.abs(F.col("ms_played") - 151112).alias("ms_played_distance"),
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

result = (
    tagged
    .groupBy("is_skipped", "device")
    .pivot("device_or_unknown", ["car", "UNKNOWN", "desktop"])
    .agg(F.max("ms_played_distance"))
)

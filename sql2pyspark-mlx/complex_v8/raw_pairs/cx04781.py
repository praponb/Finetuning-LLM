from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) | (F.col("track_id") <= 18))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

tagged = (
    recent
    .filter(F.col("listener_id") >= 14)
    .withColumn("device_prefix", F.substring(F.trim(F.col("device")), 1, 4))
)

result = (
    tagged
    .groupingSets(
        [["track_id", "listener_id", "device"], ["track_id"], []],
        "track_id", "listener_id", "device",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
    )
    .select(
        "track_id",
        "listener_id",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "cnt",
        "first_played_at",
        "avg_ms_played",
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)

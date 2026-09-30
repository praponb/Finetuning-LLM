from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((F.col("track_id") >= 22) & (F.col("device").isin("car", "speaker", "phone")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

scored = (
    valid_plays
    .select(
        "*",
        F.least(F.col("ms_played"), F.col("days_since_played_at")).alias("least_ms_played_days_since_played_at"),
    )
)

result = (
    scored
    .rollup("device", "source_or_unknown")
    .agg(F.min("ms_played").alias("min_ms_played"))
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("source_or_unknown").asc_nulls_last(),
    )
)

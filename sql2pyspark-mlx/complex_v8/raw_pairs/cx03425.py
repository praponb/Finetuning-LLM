from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

per_group = (
    scoped
    .groupBy("is_skipped", F.year("played_at").alias("played_at_year"), "track_id")
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.count("*").alias("cnt"),
    )
)

annotated = (
    per_group
    .filter(F.col("played_at_year").between(2024, 2025))
    .select(
        "is_skipped",
        "played_at_year",
        "track_id",
        "total_ms_played",
        "stddev_ms_played",
        "avg_ms_played",
        (F.col("avg_ms_played") - F.col("played_at_year")).alias("avg_ms_played_minus_played_at_year"),
        F.round(F.col("stddev_ms_played") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("stddev_ms_played_per_cnt"),
    )
)

result = (
    annotated
    .select(
        "is_skipped",
        "track_id",
        "avg_ms_played",
        "avg_ms_played_minus_played_at_year",
        "stddev_ms_played_per_cnt",
    )
    .orderBy(F.col("stddev_ms_played_per_cnt").asc(), "avg_ms_played")
)

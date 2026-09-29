from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

track_id_stats = (
    cleaned
    .groupBy("track_id")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

vs_group = (
    cleaned.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

rollup_base = (
    vs_group
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.count("*").alias("row_count"),
        F.round(F.avg("max_ms_played"), 2).alias("avg_max_ms_played"),
        F.max("max_ms_played").alias("max_max_ms_played"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)

result = (
    rollup_base
    .select(
        "track_id",
        "total_ms_played",
        "row_count",
        "avg_max_ms_played",
        "max_max_ms_played",
        "distinct_listener_ids",
    )
    .orderBy("distinct_listener_ids", F.col("total_ms_played").desc())
)

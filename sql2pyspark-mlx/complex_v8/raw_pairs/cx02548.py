from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
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

with_activity = (
    scoped
    .crossJoin(scoped.agg(F.min("ms_played").alias("overall_min_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_min_ms_played"))
    .drop("overall_min_ms_played")
)

grouped = (
    with_activity
    .filter(F.col("track_id") >= 6)
    .groupBy("track_id", "listener_id")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.min("ms_played").alias("min_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
)

annotated = (
    grouped
    .select(
        "*",
        F.hour(F.col("first_played_at")).alias("first_played_at_hour"),
        (F.col("distinct_devices") + F.col("min_ms_played")).alias("distinct_devices_plus_min_ms_played"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.track_id",
        "s.listener_id",
        "s.distinct_devices",
        "s.min_ms_played",
        "s.first_played_at_hour",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

enriched = (
    exploded_detail
    .select(
        "track_id",
        "listener_id",
        "distinct_devices",
        "min_ms_played",
        "first_played_at_hour",
        "play_id",
        "play_track_id",
        F.when(F.col("min_ms_played") >= 281500, "large").when(F.col("min_ms_played") >= 143250, "medium").otherwise("small").alias("min_ms_played_tier"),
        F.lower(F.trim(F.col("play_source"))).alias("play_source_clean"),
    )
)

result = (
    enriched
    .select(
        "track_id",
        "listener_id",
        "distinct_devices",
        "min_ms_played",
        "first_played_at_hour",
        "play_id",
        "play_track_id",
        "min_ms_played_tier",
        F.substring(F.trim(F.col("play_source_clean")), 1, 4).alias("play_source_clean_prefix"),
        F.length(F.trim(F.col("min_ms_played_tier"))).alias("min_ms_played_tier_len"),
    )
    .orderBy(
        F.col("first_played_at_hour").desc(),
        "min_ms_played_tier_len",
        "track_id",
        "listener_id",
        "play_id",
    )
    .limit(10)
)

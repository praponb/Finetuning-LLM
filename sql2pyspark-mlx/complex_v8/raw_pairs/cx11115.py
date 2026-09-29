from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("device").isin("desktop", "car"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("ms_played") >= 318978, "large").when(F.col("ms_played") >= 73177, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

per_group = (
    recent_plays
    .groupBy("device_group", "listener_id")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.avg("ms_played").alias("avg_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_check") > 98052)
    .drop("avg_ms_played_check")
)

exploded = (
    per_group
    .select(
        "device_group",
        "listener_id",
        "distinct_track_ids",
        F.explode("device_list").alias("device_list_item"),
    )
)

result = (
    exploded
    .select(
        "device_group",
        "listener_id",
        "distinct_track_ids",
        "device_list_item",
    )
    .orderBy(
        "device_group",
        F.col("listener_id").desc_nulls_first(),
        "device_list_item",
    )
    .limit(10)
)

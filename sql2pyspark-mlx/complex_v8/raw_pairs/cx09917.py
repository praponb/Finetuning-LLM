from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("track_id") >= 10) & (F.col("ms_played") <= 223262))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

enriched = (
    filtered_plays
    .filter(~F.col("is_skipped"))
    .select(
        "*",
        F.abs(F.col("ms_played") - 99376).alias("ms_played_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.lower(F.trim(F.col("device_slug"))).alias("device_slug_clean"),
    )
)

totals = (
    enriched
    .filter(F.col("source_slug").isNotNull())
    .groupBy("is_skipped")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.round(100 * F.count(F.when(F.col("source").isNotNull(), 1)) / F.count("*"), 1).alias("pct_source_is_not_null"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("ms_played_distance"), F.lit(0)), 3).alias("ms_played_to_ms_played_distance_ratio"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.countDistinct("source").alias("distinct_sources"),
        F.sum("ms_played_distance").alias("sum_ms_played_distance_check"),
    )
    .filter(F.col("sum_ms_played_distance_check") > 40000)
    .drop("sum_ms_played_distance_check")
)

result = (
    totals
    .select(
        "is_skipped",
        "distinct_track_ids",
        "ms_played_to_ms_played_distance_ratio",
    )
    .orderBy(
        F.col("ms_played_to_ms_played_distance_ratio").desc_nulls_first(),
        "distinct_track_ids",
    )
)

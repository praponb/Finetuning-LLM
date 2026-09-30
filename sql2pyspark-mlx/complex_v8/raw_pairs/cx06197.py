from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

source_stats = (
    recent_plays
    .groupBy("source")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

above_average = (
    recent_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

result = (
    above_average
    .rollup("source_group", "source")
    .agg(
        F.grouping("source_group").alias("is_total_source_group"),
        F.sum("ms_played_minus_median").alias("total_ms_played_minus_median"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
    .select(
        F.coalesce(F.col("source_group"), F.lit("ALL")).alias("source_group"),
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "is_total_source_group",
        "total_ms_played_minus_median",
        "device_list",
    )
    .orderBy(
        F.col("source_group").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
    )
)

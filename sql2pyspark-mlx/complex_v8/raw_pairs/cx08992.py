from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("listener_id") <= 12)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

summary = (
    filtered_plays
    .groupBy("device", "source_len", "source")
    .agg(
        F.sum("source_len").alias("total_source_len"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(100 * F.count(F.when(F.col("device") == "desktop", 1)) / F.count("*"), 1).alias("pct_device_desktop"),
        F.round(100 * F.count(F.when(F.col("device").isNotNull(), 1)) / F.count("*"), 1).alias("pct_device_is_not_null"),
    )
)

exploded = (
    summary
    .select(
        "device",
        "source_len",
        "source",
        "total_source_len",
        "row_count",
        "pct_device_desktop",
        "pct_device_is_not_null",
        F.explode("source_list").alias("source_list_item"),
    )
)

screened = (
    exploded
    .crossJoin(exploded.agg(F.avg("source_len").alias("overall_avg_source_len")))
    .filter(F.col("source_len") > F.col("overall_avg_source_len"))
    .drop("overall_avg_source_len")
)

result = (
    screened
    .select(
        "device",
        "source_len",
        "source",
        "row_count",
        "pct_device_desktop",
        "source_list_item",
    )
    .orderBy(F.col("source_list_item").asc())
)

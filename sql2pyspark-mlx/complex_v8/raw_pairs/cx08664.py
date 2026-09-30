from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("source").isin("album", "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("phone", "desktop"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)

enriched = (
    base
    .select(
        "id",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
        F.concat_ws(" / ", F.col("device_group"), F.col("source")).alias("device_group_source_label"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.length(F.trim(F.col("source_clean"))).alias("source_clean_len"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

source_stats = (
    labeled
    .groupBy("source")
    .agg(F.percentile_approx("source_clean_len", 0.5).alias("median_source_clean_len"))
)

above_average = (
    labeled.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.source_clean_len") >= F.col("g.median_source_clean_len"))
    .select(
        "s.*",
        "g.median_source_clean_len",
        F.round(F.col("s.source_clean_len") - F.col("g.median_source_clean_len"), 2).alias("source_clean_len_minus_median"),
    )
)

qualified = (
    above_average
    .crossJoin(above_average.agg(F.min("median_source_clean_len").alias("overall_min_median_source_clean_len")))
    .filter(F.col("median_source_clean_len") > F.col("overall_min_median_source_clean_len"))
    .drop("overall_min_median_source_clean_len")
)

enriched_2 = (
    qualified
    .select(
        "*",
        F.when(F.col("is_skipped_flag") >= 1, "A").when(F.col("is_skipped_flag") >= 0, "B").otherwise("C").alias("is_skipped_flag_tier"),
        F.when(F.col("median_source_clean_len") >= 9.8, "HIGH").when(F.col("median_source_clean_len") >= 7.4, "MEDIUM").otherwise("LOW").alias("median_source_clean_len_tier"),
        (F.floor(F.col("source_clean_len") / 2) * 2).alias("source_clean_len_bucket"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "is_skipped",
        "source",
        "source_clean",
        "device_group_source_label",
        "source_clean_len",
        "is_skipped_flag",
        "median_source_clean_len",
        "source_clean_len_minus_median",
        "is_skipped_flag_tier",
        "median_source_clean_len_tier",
        "source_clean_len_bucket",
    )
)

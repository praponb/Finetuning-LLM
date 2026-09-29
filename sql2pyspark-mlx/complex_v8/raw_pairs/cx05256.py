from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%bk%"))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("capacity").cast("string")).alias("wh_code_ref"),
        F.when(F.col("capacity") >= 11768, "large").when(F.col("capacity") >= 4912, "medium").otherwise("small").alias("capacity_tier"),
    )
)

capacity_tier_stats = (
    cleaned_warehouses
    .groupBy("capacity_tier")
    .agg(F.percentile_approx("capacity", 0.5).alias("median_capacity"))
)

outliers = (
    cleaned_warehouses.alias("s")
    .join(capacity_tier_stats.alias("g"), F.col("g.capacity_tier") == F.col("s.capacity_tier"), "inner")
    .filter(F.col("s.capacity") >= F.col("g.median_capacity"))
    .select(
        "s.*",
        "g.median_capacity",
        F.round(F.col("s.capacity") - F.col("g.median_capacity"), 2).alias("capacity_minus_median"),
    )
)

capacity_tier_stats_2 = (
    outliers
    .groupBy("capacity_tier")
    .agg(F.percentile_approx("capacity_minus_median", 0.5).alias("median_capacity_minus_median"))
)

above_average = (
    outliers.alias("s")
    .join(capacity_tier_stats_2.alias("g"), F.col("g.capacity_tier") == F.col("s.capacity_tier"), "inner")
    .filter(F.col("s.capacity_minus_median") >= F.col("g.median_capacity_minus_median"))
    .select(
        "s.*",
        "g.median_capacity_minus_median",
        F.round(F.col("s.capacity_minus_median") - F.col("g.median_capacity_minus_median"), 2).alias("capacity_minus_median_minus_median"),
    )
)

scored = (
    above_average
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "id",
        "wh_code",
        "opened_on",
        "wh_code_ref",
        "capacity_tier",
        "capacity_minus_median",
        "median_capacity_minus_median",
        "capacity_minus_median_minus_median",
        F.when(F.col("median_capacity") >= 13350.0, "large").when(F.col("median_capacity") >= 6700.0, "medium").otherwise("small").alias("median_capacity_tier"),
        F.concat(F.col("wh_code_ref"), F.lit("#"), F.col("capacity").cast("string")).alias("wh_code_ref_ref"),
        F.when(F.col("capacity_tier").isin("large", "medium"), "group_a").when(F.col("capacity_tier") == "small", "group_b").otherwise("other").alias("capacity_tier_group"),
    )
)

result = (
    scored
    .groupBy("opened_on", "median_capacity_tier")
    .pivot("capacity_tier_group", ["group_b", "other"])
    .agg(F.avg("capacity_minus_median_minus_median").alias("avg_v"), F.count("capacity_minus_median_minus_median").alias("n"))
    .orderBy("opened_on", "median_capacity_tier")
)

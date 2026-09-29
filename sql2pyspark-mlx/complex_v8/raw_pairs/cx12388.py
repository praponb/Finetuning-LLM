from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("%ru%"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.year(F.col("hired_on")).alias("hired_on_year"),
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

tier_stats = (
    cleaned
    .groupBy("tier")
    .agg(F.percentile_approx("hired_on_year", 0.5).alias("median_hired_on_year"))
)

vs_group = (
    cleaned.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.hired_on_year") >= F.col("g.median_hired_on_year"))
    .select(
        "s.*",
        "g.median_hired_on_year",
        F.round(F.col("s.hired_on_year") - F.col("g.median_hired_on_year"), 2).alias("hired_on_year_minus_median"),
    )
)

tagged = (
    vs_group
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_year",
        "tier_or_unknown",
        "median_hired_on_year",
        "hired_on_year_minus_median",
        (F.col("median_hired_on_year") + F.col("hired_on_year")).alias("median_hired_on_year_plus_hired_on_year"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label_2"),
        F.when(F.col("median_hired_on_year") >= 2025.4, "HIGH").when(F.col("median_hired_on_year") >= 2024.4, "MEDIUM").otherwise("LOW").alias("median_hired_on_year_tier"),
    )
)

filtered = (
    tagged
    .crossJoin(tagged.agg(F.avg("hired_on_year_minus_median").alias("overall_avg_hired_on_year_minus_median")))
    .filter(F.col("hired_on_year_minus_median") > F.col("overall_avg_hired_on_year_minus_median"))
    .drop("overall_avg_hired_on_year_minus_median")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.max("median_hired_on_year_plus_hired_on_year").alias("overall_max_median_hired_on_year_plus_hired_on_y")))
    .filter(F.col("median_hired_on_year_plus_hired_on_year") < F.col("overall_max_median_hired_on_year_plus_hired_on_y"))
    .drop("overall_max_median_hired_on_year_plus_hired_on_y")
)

annotated = (
    without_matches
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_year",
        "tier_or_unknown",
        "median_hired_on_year",
        "hired_on_year_minus_median",
        "median_hired_on_year_plus_hired_on_year",
        "hired_on_label_2",
        F.substring(F.trim(F.col("hired_on_label_2")), 1, 3).alias("hired_on_label_2_prefix"),
    )
)

result = (
    annotated
    .select(
        "id",
        "hired_on",
        "tier_or_unknown",
        "median_hired_on_year",
        "hired_on_year_minus_median",
        "hired_on_label_2",
        F.length(F.trim(F.col("hired_on_label_2_prefix"))).alias("hired_on_label_2_prefix_len"),
        F.when(F.col("hired_on_year") >= 2026, "A").when(F.col("hired_on_year") >= 2025, "B").otherwise("C").alias("hired_on_year_tier"),
    )
    .orderBy(F.col("hired_on_year_minus_median").desc(), "median_hired_on_year")
)

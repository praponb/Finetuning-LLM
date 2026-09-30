from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

screened = (
    recent_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("PAID", "REJECTED", "OPEN")) & (F.col("c.claim_date").between(F.lit("2024-06-07").cast("date"), F.lit("2025-10-15").cast("date"))), "left_semi")
)

product_or_unknown_stats = (
    screened
    .groupBy("product_or_unknown")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    screened.alias("s")
    .join(product_or_unknown_stats.alias("g"), F.col("g.product_or_unknown") == F.col("s.product_or_unknown"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

stats = (
    vs_group
    .groupBy("product_or_unknown")
    .agg(
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.round(F.sum("avg_premium"), 2).alias("total_avg_premium"),
        F.count("*").alias("row_count"),
        F.round(F.sum("avg_premium") / F.nullif(F.sum("premium_minus_avg"), F.lit(0)), 3).alias("avg_premium_to_premium_minus_avg_ratio"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.max("row_count").alias("overall_max_row_count")))
    .filter(F.col("row_count") < F.col("overall_max_row_count"))
    .drop("overall_max_row_count")
)

filtered = (
    without_matches
    .crossJoin(without_matches.agg(F.max("total_avg_premium").alias("overall_max_total_avg_premium")))
    .filter(F.col("total_avg_premium") < F.col("overall_max_total_avg_premium"))
    .drop("overall_max_total_avg_premium")
)

without_matches_2 = (
    filtered
    .crossJoin(filtered.agg(F.avg("distinct_products").alias("overall_avg_distinct_products")))
    .filter(F.col("distinct_products") > F.col("overall_avg_distinct_products"))
    .drop("overall_avg_distinct_products")
)

result = (
    without_matches_2
    .filter(F.col("stddev_premium") >= 1250.0)
    .select(
        "product_or_unknown",
        "stddev_premium",
        "total_avg_premium",
        "avg_premium_to_premium_minus_avg_ratio",
        (F.floor(F.col("total_avg_premium") / 5000) * 5000).alias("total_avg_premium_bucket"),
    )
    .orderBy(F.col("product_or_unknown").desc_nulls_first(), "stddev_premium")
)

from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter((F.length(F.trim(F.col("holder_name"))) >= 5) & (F.col("joined_on").isNotNull()))
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
        F.quarter(F.col("joined_on")).alias("joined_on_quarter"),
    )
)

derived = (
    base
    .filter(F.col("joined_on_quarter") <= 3)
    .select(
        "*",
        F.length(F.trim(F.col("region"))).alias("region_len"),
        F.when(F.col("segment").isin("SME", "VIP"), "group_a").when(F.col("segment") == "RETAIL", "group_b").otherwise("other").alias("segment_group"),
    )
)

scored = (
    derived
    .filter(F.col("segment").isin("RETAIL", "SME"))
    .select(
        "*",
        F.upper(F.trim(F.col("region"))).alias("region_clean"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.round(F.col("days_since_joined_on") / F.nullif(F.col("risk_score"), F.lit(0)), 3).alias("days_since_joined_on_per_risk_score"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("product") != "LIFE", 1)).alias("policy_product_count"),
    )
)

with_stats = (
    scored.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

result = (
    with_stats
    .rollup("policy_product_count", "region", "segment_group")
    .agg(
        F.array_sort(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("days_since_joined_on") / F.nullif(F.sum("policy_product_count"), F.lit(0)), 3).alias("days_since_joined_on_to_policy_product_count_rat"),
    )
    .orderBy(
        F.col("policy_product_count").asc_nulls_last(),
        F.col("region").asc_nulls_last(),
        F.col("segment_group").asc_nulls_last(),
    )
)

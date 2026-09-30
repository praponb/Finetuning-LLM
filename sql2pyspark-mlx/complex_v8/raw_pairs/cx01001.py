from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("product") != "TRAVEL")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.holder_name").isNotNull()), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

stats = (
    with_counts
    .groupBy("product")
    .agg(
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.count(F.when(F.col("product") != "AUTO", 1)).alias("n_product_auto"),
        F.sum("claim_count").alias("sum_claim_count_check"),
    )
    .filter(F.col("sum_claim_count_check") > 3)
    .drop("sum_claim_count_check")
)

result = (
    stats
    .filter(F.col("n_product_auto") >= 2)
    .select("product", "total_premium")
    .orderBy(F.col("product").desc_nulls_first())
)

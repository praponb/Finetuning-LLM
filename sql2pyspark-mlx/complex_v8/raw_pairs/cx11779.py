from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("holder_id") >= 18)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.date_add(F.col("start_date"), 90).alias("start_date_plus_90d"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "medical", 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

derived = (
    with_stats
    .select(
        "*",
        F.least(F.col("premium_bucket"), F.col("status_len")).alias("least_premium_bucket_status_len"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

enriched = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") < 0.6)
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.premium_bucket",
        "s.start_date_plus_90d",
        "s.claim_claim_type_count",
        "s.least_premium_bucket_status_len",
        "s.product_len",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    enriched
    .filter(F.col("least_premium_bucket_status_len") > 1751)
    .select(
        "id",
        "premium_bucket",
        "start_date_plus_90d",
        "product_len",
        F.date_format(F.col("start_date_plus_90d"), "yyyy-MM-dd").alias("start_date_plus_90d_label"),
    )
)

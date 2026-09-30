from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

enriched_policy = (
    valid_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.year(F.col("p.start_date")) == 2025), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.status").isNotNull())
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        "po.holder_name",
        F.col("po.region").alias("holder_region"),
    )
)

with_details = (
    enriched_policy.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team") == "Property")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.status_ref",
        "s.approved_amount_bucket",
        "s.policy_product",
        "s.policy_status",
        "s.holder_name",
        "s.holder_region",
        F.col("a.team").alias("adjuster_team"),
    )
)

exploded_detail = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.status_ref",
        "s.policy_product",
        "s.holder_name",
        "s.adjuster_team",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

policy_product_stats = (
    exploded_detail
    .groupBy("policy_product")
    .agg(F.avg("claim_claimed_amount").alias("avg_claim_claimed_amount"))
)

above_average = (
    exploded_detail.alias("s")
    .join(policy_product_stats.alias("g"), F.col("g.policy_product") == F.col("s.policy_product"), "inner")
    .filter(F.col("s.claim_claimed_amount") > F.col("g.avg_claim_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claim_claimed_amount",
        F.round(F.col("s.claim_claimed_amount") - F.col("g.avg_claim_claimed_amount"), 2).alias("claim_claimed_amount_minus_avg"),
    )
)

derived = above_average.select("*", F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"))

scored = (
    derived
    .filter(F.col("claim_claimed_amount_minus_avg") > -40000.0)
    .withColumn("policy_product_prefix", F.substring(F.trim(F.col("policy_product")), 1, 2))
)

result = (
    scored
    .cube("policy_id", "policy_product")
    .agg(
        F.max("avg_claim_claimed_amount").alias("max_avg_claim_claimed_amount"),
    )
)

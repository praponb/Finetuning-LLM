from pyspark.sql import functions as F

base = (
    df_claims
    .filter(F.col("status") != "OPEN")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("claimed_amount").alias("max_claim_claimed_amount"))
)

with_stats = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.max_claim_claimed_amount")
)

enriched = (
    with_stats.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.status_clean",
        "s.approved_amount_per_claimed_amount",
        "s.max_claim_claimed_amount",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-03-02").cast("date")), "left_anti")
    .filter(F.col("adjuster_id").isNotNull())
)

rollup_base = (
    screened
    .filter(F.col("max_claim_claimed_amount") < 40020.0)
    .groupBy("policy_status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("policy_status").isNotNull(), F.col("policy_premium")).otherwise(0)).alias("policy_premium_policy_status_is_not_null"),
        F.countDistinct("policy_holder_id").alias("distinct_policy_holder_ids"),
    )
)

result = (
    rollup_base
    .filter(F.col("policy_status").isin("LAPSED", "ACTIVE", "CANCELLED"))
    .select(
        "policy_status",
        "distinct_ids",
        "policy_premium_policy_status_is_not_null",
        "distinct_policy_holder_ids",
    )
)

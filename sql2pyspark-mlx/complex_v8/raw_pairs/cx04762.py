from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 19337.6) & (F.col("status") == "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
        F.when(F.col("status").isin("IN_REVIEW", "OPEN"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
    )
)

exploded_detail_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.days_since_claim_date",
        "s.status_group",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

enriched_claim = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status_group",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_status",
        "s.claim_claimed_amount",
        "s.claim_policy_id",
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
    )
)

result = (
    enriched_claim
    .groupingSets(
        [["claim_status", "claim_policy_id", "policy_id"], ["claim_status"], ["claim_policy_id"], []],
        "claim_status", "claim_policy_id", "policy_id",
    )
    .agg(
        F.grouping("claim_status").alias("is_total_claim_status"),
        F.round(F.sum("claim_claimed_amount_2") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claim_claimed_amount_2_to_approved_amount_ratio"),
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
        F.max("claim_reported_at").alias("last_claim_reported_at"),
    )
    .select(
        F.coalesce(F.col("claim_status"), F.lit("ALL")).alias("claim_status"),
        "claim_policy_id",
        "policy_id",
        "is_total_claim_status",
        "claim_claimed_amount_2_to_approved_amount_ratio",
        "distinct_claim_ids",
        "last_claim_reported_at",
    )
)

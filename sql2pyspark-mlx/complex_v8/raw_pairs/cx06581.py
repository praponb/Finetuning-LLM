from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

labeled = (
    filtered
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount_2"),
    )
)

labeled_2 = (
    labeled
    .filter(F.col("claim_date").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

exploded_detail = (
    labeled_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.claimed_amount_per_approved_amount_2",
        "s.status_slug",
        "s.status_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

enriched = (
    exploded_detail.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.holder_id") <= 11)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.claimed_amount_per_approved_amount_2",
        "s.status_slug",
        "s.status_prefix",
        "s.claim_id",
        "s.claim_status",
        "s.claim_policy_id",
        "s.claim_claim_date",
        "s.claim_reported_at",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_per_approved_amount_2",
        "status_slug",
        "status_prefix",
        "claim_id",
        "claim_status",
        "claim_policy_id",
        "claim_claim_date",
        "claim_reported_at",
        "policy_premium",
        "policy_product",
        F.date_trunc("month", F.col("claim_reported_at")).alias("claim_reported_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug_2"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("status_slug").asc_nulls_last(),
        "id",
        "claim_id",
    )
    .limit(5)
)

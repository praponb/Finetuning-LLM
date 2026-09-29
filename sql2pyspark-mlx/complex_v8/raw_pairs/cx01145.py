from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

combined_details_policy = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isin("TRAVEL", "LIFE")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

with_parent = (
    combined_details_policy.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.policy_status",
        "s.policy_product",
        "s.policy_holder_id",
        F.col("p.premium").alias("policy_premium"),
    )
)

tagged = (
    with_parent
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.greatest(F.col("policy_premium"), F.col("claimed_amount")).alias("greatest_policy_premium_claimed_amount"),
    )
)

enriched = (
    tagged.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.policy_status",
        "s.policy_product",
        "s.policy_holder_id",
        "s.policy_premium",
        "s.status_slug",
        "s.greatest_policy_premium_claimed_amount",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
    )
)

detail_rows_claims = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.policy_status",
        "s.policy_holder_id",
        "s.policy_premium",
        "s.status_slug",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_holder_id",
        "claim_id",
        "claim_reported_at",
        "claim_status",
        F.when(F.col("policy_premium") >= 3544.1, "large").when(F.col("policy_premium") >= 2013.8, "medium").otherwise("small").alias("policy_premium_tier"),
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last())
)

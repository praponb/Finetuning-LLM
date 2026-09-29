from pyspark.sql import functions as F

base = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(7, 9))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

with_children_claims = (
    base.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claim_type_clean",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

with_details_policy = (
    with_children_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claim_type_clean",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_date",
        "s.claim_status",
        "s.claim_claim_type",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

filtered = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.hour(F.col("c.reported_at")).between(9, 13)), "left_anti")
)

result = (
    filtered
    .cube("policy_product", "claim_claim_type", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("claim_date").alias("first_claim_date"),
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        F.coalesce(F.col("claim_claim_type"), F.lit("ALL")).alias("claim_claim_type"),
        "policy_id",
        "gid",
        "first_claim_date",
        "stddev_claimed_amount",
    )
    .orderBy(
        F.col("policy_product").asc_nulls_last(),
        F.col("claim_claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

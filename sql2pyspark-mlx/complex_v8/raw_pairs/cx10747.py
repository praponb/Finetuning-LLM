from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

with_children = (
    scoped_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claimed_amount_bucket",
        "s.claimed_amount_plus_approved_amount",
        "s.status_ref",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

with_children_2 = (
    with_children.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.claim_policy_id")) & (F.col("c.approved_amount") < 15477.7), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claimed_amount_bucket",
        "s.status_ref",
        "s.claim_id",
        "s.claim_policy_id",
        "s.claim_claim_date",
        "s.claim_claim_type",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    with_children_2
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "claimed_amount_bucket",
        "status_ref",
        "claim_policy_id",
        "claim_claim_date",
        "claim_claim_type",
        "claim_id_2",
        "claim_approved_amount",
        F.when(F.col("claim_claim_type").isin("medical", "collision"), "group_a").when(F.col("claim_claim_type") == "fire", "group_b").otherwise("other").alias("claim_claim_type_group"),
    )
    .orderBy(F.col("reported_at").asc_nulls_last(), F.col("approved_amount").asc())
)

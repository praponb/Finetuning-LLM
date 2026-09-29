from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("claimed_amount") < 42447.2)
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
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
)

with_details_policy = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.status_or_unknown",
        "s.claim_date_label",
        "s.claim_type_ref",
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

result = (
    with_details_policy
    .groupingSets(
        [["policy_product", "adjuster_id", "status_or_unknown"], ["policy_product"], ["adjuster_id"]],
        "policy_product", "adjuster_id", "status_or_unknown",
    )
    .agg(
        F.grouping("policy_product").alias("is_total_policy_product"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claimed_amount_to_approved_amount_ratio"),
        F.sum(F.when(F.col("claim_type") == "fire", 1).otherwise(0)).alias("n_claim_type_fire"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        "adjuster_id",
        F.coalesce(F.col("status_or_unknown"), F.lit("ALL")).alias("status_or_unknown"),
        "is_total_policy_product",
        "claimed_amount_to_approved_amount_ratio",
        "n_claim_type_fire",
        "distinct_status",
    )
)

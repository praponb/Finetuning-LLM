from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.when(F.col("approved_amount") >= 23487.5, "HIGH").when(F.col("approved_amount") >= 799.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

combined_details_policy = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.region").alias("holder_region"),
        "po.holder_name",
    )
)

tagged = (
    combined_details_policy
    .filter(F.col("holder_name").isNotNull())
    .select(
        "*",
        F.concat(F.col("holder_name"), F.lit("#"), F.col("id").cast("string")).alias("holder_name_ref"),
        F.when(F.col("claim_type").isin("delay", "fire"), "group_a").when(F.col("claim_type") == "flood", "group_b").otherwise("other").alias("claim_type_group"),
        F.when(F.col("status").isin("OPEN", "REJECTED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

result = (
    tagged
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "policy_end_date",
        "policy_holder_id",
        "holder_region",
        "holder_name",
        "holder_name_ref",
        "claim_type_group",
        "status_group",
        F.date_format(F.col("policy_end_date"), "yyyy-MM-dd").alias("policy_end_date_label"),
    )
    .orderBy(F.col("id").desc(), F.col("policy_end_date").desc_nulls_first())
)

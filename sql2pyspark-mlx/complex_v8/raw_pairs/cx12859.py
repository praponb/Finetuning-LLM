from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("approved_amount") > 0.0) & (F.col("claim_type").isin("collision", "fire", "flood")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.when(F.col("status").isin("APPROVED", "IN_REVIEW"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
    )
)

combined_details_policy = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

without_matches = (
    combined_details_policy
    .crossJoin(combined_details_policy.agg(F.min("approved_amount_per_claimed_amount").alias("overall_min_approved_amount_per_claimed_amount")))
    .filter(F.col("approved_amount_per_claimed_amount") > F.col("overall_min_approved_amount_per_claimed_amount"))
    .drop("overall_min_approved_amount_per_claimed_amount")
)

result = (
    without_matches
    .groupBy("status")
    .pivot("claim_type", ["delay", "fire", "theft", "medical"])
    .agg(F.max("approved_amount_per_claimed_amount"))
    .orderBy("status")
)

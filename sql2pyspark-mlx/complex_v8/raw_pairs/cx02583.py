from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_type").isin("theft", "flood", "collision"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.when(F.col("claim_type").isin("collision", "theft"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

rollup_base = (
    recent_claims
    .filter(F.col("adjuster_id").isNotNull())
    .groupBy("adjuster_id")
    .agg(
        F.sum(F.when(F.col("status") != "APPROVED", 1).otherwise(0)).alias("n_status_approved"),
        F.round(100 * F.count(F.when(F.col("status").isin("REJECTED", "APPROVED"), 1)) / F.count("*"), 1).alias("pct_status_in_rejected_approved"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.avg("claimed_amount").alias("avg_claimed_amount_check"),
    )
    .filter(F.col("avg_claimed_amount_check") > 11235.6)
    .drop("avg_claimed_amount_check")
)

derived = (
    rollup_base
    .select(
        "*",
        F.greatest(F.col("n_status_approved"), F.col("distinct_policy_ids")).alias("greatest_n_status_approved_distinct_policy_ids"),
    )
)

exploded_detail_claims = (
    derived.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.adjuster_id",
        "s.pct_status_in_rejected_approved",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    exploded_detail_claims
    .select(
        "adjuster_id",
        "claim_reported_at",
        F.to_date(F.col("claim_reported_at")).alias("claim_reported_at_date"),
        F.hour(F.col("claim_reported_at")).alias("claim_reported_at_hour"),
    )
    .orderBy("claim_reported_at", "claim_reported_at_date")
)

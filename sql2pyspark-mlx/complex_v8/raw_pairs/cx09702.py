from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("claim_type") == "fire")
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
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

exploded_detail_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.least_approved_amount_claimed_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

joined = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .filter(F.col("c.status").isin("APPROVED", "OPEN", "PAID"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.least_approved_amount_claimed_amount",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_status",
        "s.claim_claim_type",
        F.col("c.status").alias("claim_status_2"),
    )
)

tagged = (
    joined
    .withColumn("approved_amount_plus_claimed_amount", F.col("approved_amount") + F.col("claimed_amount"))
)

grouped = (
    tagged
    .filter(F.col("least_approved_amount_claimed_amount") <= 40000.0)
    .groupBy("policy_id", "claim_adjuster_id")
    .agg(
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claimed_amount_to_approved_amount_ratio"),
        F.countDistinct("claim_adjuster_id").alias("distinct_claim_adjuster_ids"),
        F.count("*").alias("cnt"),
        F.count(F.when(~F.col("claim_status_2").isin("REJECTED", "IN_REVIEW"), 1)).alias("n_claim_status_2_not_in_reject"),
        F.countDistinct("claim_claim_type").alias("distinct_claim_claim_types"),
    )
)

screened = (
    grouped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.col("c.approved_amount") >= 2887.3) & (F.col("c.reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")), "left_anti")
)

result = (
    screened
    .select(
        "policy_id",
        "claim_adjuster_id",
        "claimed_amount_to_approved_amount_ratio",
        "distinct_claim_adjuster_ids",
        "cnt",
        "n_claim_status_2_not_in_reject",
        "distinct_claim_claim_types",
    )
    .orderBy(
        F.col("claimed_amount_to_approved_amount_ratio").desc(),
        F.col("n_claim_status_2_not_in_reject").desc(),
        "policy_id",
        "claim_adjuster_id",
    )
    .limit(5)
)

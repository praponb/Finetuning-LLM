from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.col("status") == "PAID"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

combined_details_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claim_date_quarter",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

scored = (
    combined_details_adjuster
    .filter(F.col("policy_premium") < 2945.9)
    .select(
        "*",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_stats = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.max_claim_approved_amount")
)

result = (
    with_stats
    .groupBy("last_claim_reported_at", "claim_date_quarter")
    .pivot("policy_status", ["ACTIVE", "LAPSED", "CANCELLED"])
    .agg(F.sum("adjuster_level"))
    .orderBy("last_claim_reported_at", "claim_date_quarter")
)

from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.year(F.col("claim_date")) == 2025))
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
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
)

exploded_detail = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at_plus_1d",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_counts = (
    exploded_detail.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_claimed_amount",
        "c.max_claim_approved_amount",
        "c.last_claim_date",
    )
)

result = (
    with_counts
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at_plus_1d",
        "claim_id",
        "claim_reported_at",
        "claim_claimed_amount",
        "claim_policy_id",
        "avg_claim_claimed_amount",
        "last_claim_date",
        F.when(F.col("approved_amount") >= 20843.1, "HIGH").when(F.col("approved_amount") >= 5336.7, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "claim_id")
    .limit(10)
)

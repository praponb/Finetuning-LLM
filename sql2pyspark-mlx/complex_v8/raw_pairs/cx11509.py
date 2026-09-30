from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_type") == "medical")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

profiled = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
        "c.avg_claim_approved_amount",
    )
)

joined_policy = (
    profiled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") < 3719.7), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_count",
        "s.last_claim_reported_at",
        "s.avg_claim_approved_amount",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    joined_policy
    .groupingSets(
        [["policy_id", "status"], ["policy_id"], ["status"]],
        "policy_id", "status",
    )
    .agg(
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
        F.max("last_claim_reported_at").alias("last_last_claim_reported_at"),
        F.round(F.avg("claim_count"), 2).alias("avg_claim_count"),
    )
    .orderBy(F.col("policy_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)

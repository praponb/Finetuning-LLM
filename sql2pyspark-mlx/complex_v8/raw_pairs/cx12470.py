from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("adjuster_id") >= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

exploded_detail = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") > 0.0), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    exploded_detail
    .rollup("claim_status", "adjuster_id")
    .agg(
        F.grouping("claim_status").alias("is_total_claim_status"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("claim_status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)

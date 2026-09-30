from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.col("status").isNotNull()) & (F.col("claim_type") == "delay"))
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
    )
)

detail_rows_claims = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    detail_rows_claims
    .groupingSets(
        [["adjuster_id", "policy_id", "claim_id"], ["adjuster_id"], []],
        "adjuster_id", "policy_id", "claim_id",
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 3)
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

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
    )
)

with_stats = (
    prepared_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_date", "c.min_claim_approved_amount")
)

result = (
    with_stats
    .cube("policy_id", "adjuster_id", "claim_type")
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.round(F.sum("min_claim_approved_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("min_claim_approved_amount_to_approved_amount_rat"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)

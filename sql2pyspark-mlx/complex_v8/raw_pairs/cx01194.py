from pyspark.sql import functions as F

approved_by_policy = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("approved_count"),
        F.sum("claimed_amount").alias("approved_total_claimed_amount"),
    )
)

paid_by_policy = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.sum("claimed_amount").alias("paid_total_claimed_amount"),
    )
)

result = (
    approved_by_policy.alias("a")
    .join(paid_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.approved_count"), F.lit(0)).alias("approved_count"),
        F.col("a.approved_total_claimed_amount"),
        F.coalesce(F.col("b.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("b.paid_total_claimed_amount"),
        (F.coalesce(F.col("a.approved_count"), F.lit(0)) - F.coalesce(F.col("b.paid_count"), F.lit(0))).alias("approved_count_minus_paid_count"),
    )
    .orderBy("policy_id")
)

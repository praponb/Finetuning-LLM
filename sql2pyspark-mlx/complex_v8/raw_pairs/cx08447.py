from pyspark.sql import functions as F

rejected_by_policy = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

approved_by_policy = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("approved_count"),
        F.sum("claimed_amount").alias("approved_total_claimed_amount"),
    )
)

result = (
    rejected_by_policy.alias("a")
    .join(approved_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.coalesce(F.col("b.approved_total_claimed_amount"), F.lit(0)).alias("approved_total_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("policy_id")
)

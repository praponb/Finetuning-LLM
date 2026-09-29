from pyspark.sql import functions as F

paid_by_policy = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.round(F.avg("claimed_amount"), 2).alias("paid_avg_claimed_amount"),
    )
)

rejected_by_policy = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

result = (
    paid_by_policy.alias("a")
    .join(rejected_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("a.paid_avg_claimed_amount"),
        F.coalesce(F.col("b.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("b.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.rejected_count"), F.lit(0))).alias("paid_count_minus_rejected_count"),
    )
    .orderBy("policy_id")
)

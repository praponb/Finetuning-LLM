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

open_by_policy = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("open_count"),
        F.round(F.avg("claimed_amount"), 2).alias("open_avg_claimed_amount"),
    )
)

result = (
    approved_by_policy.alias("a")
    .join(open_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.approved_count"), F.lit(0)).alias("approved_count"),
        F.coalesce(F.col("a.approved_total_claimed_amount"), F.lit(0)).alias("approved_total_claimed_amount"),
        F.coalesce(F.col("b.open_count"), F.lit(0)).alias("open_count"),
        F.coalesce(F.col("b.open_avg_claimed_amount"), F.lit(0)).alias("open_avg_claimed_amount"),
        (F.coalesce(F.col("a.approved_count"), F.lit(0)) - F.coalesce(F.col("b.open_count"), F.lit(0))).alias("approved_count_minus_open_count"),
    )
    .orderBy("policy_id")
)

from pyspark.sql import functions as F

open_by_policy = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("open_count"),
        F.sum("claimed_amount").alias("open_total_claimed_amount"),
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
    open_by_policy.alias("a")
    .join(approved_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.open_count"), F.lit(0)).alias("open_count"),
        F.coalesce(F.col("a.open_total_claimed_amount"), F.lit(0)).alias("open_total_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.coalesce(F.col("b.approved_total_claimed_amount"), F.lit(0)).alias("approved_total_claimed_amount"),
        (F.coalesce(F.col("a.open_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("policy_id")
)

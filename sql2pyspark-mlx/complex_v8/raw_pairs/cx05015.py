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

open_by_policy = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("open_count"),
        F.sum("claimed_amount").alias("open_total_claimed_amount"),
    )
)

result = (
    rejected_by_policy.alias("a")
    .join(open_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        F.coalesce(F.col("b.open_count"), F.lit(0)).alias("open_count"),
        F.col("b.open_total_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.open_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("policy_id")
)

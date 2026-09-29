from pyspark.sql import functions as F

open_by_policy = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("open_count"),
        F.round(F.avg("claimed_amount"), 2).alias("open_avg_claimed_amount"),
    )
)

rejected_by_policy = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.round(F.avg("claimed_amount"), 2).alias("rejected_avg_claimed_amount"),
    )
)

result = (
    open_by_policy.alias("a")
    .join(rejected_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.open_count"), F.lit(0)).alias("open_count"),
        F.col("a.open_avg_claimed_amount"),
        F.coalesce(F.col("b.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.col("b.rejected_avg_claimed_amount"),
        (F.coalesce(F.col("a.open_count"), F.lit(0)) - F.coalesce(F.col("b.rejected_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("policy_id")
)

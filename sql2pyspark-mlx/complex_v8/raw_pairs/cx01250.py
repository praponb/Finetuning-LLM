from pyspark.sql import functions as F

paid_by_policy = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.sum("claimed_amount").alias("paid_total_claimed_amount"),
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
    paid_by_policy.alias("a")
    .join(open_by_policy.alias("b"), F.col("a.policy_id") == F.col("b.policy_id"), "full")
    .select(
        F.coalesce(F.col("a.policy_id"), F.col("b.policy_id")).alias("policy_id"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("a.paid_total_claimed_amount"),
        F.coalesce(F.col("b.open_count"), F.lit(0)).alias("open_count"),
        F.col("b.open_avg_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.open_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("policy_id")
)

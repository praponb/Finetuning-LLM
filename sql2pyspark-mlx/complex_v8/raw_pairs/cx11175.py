from pyspark.sql import functions as F

paid_by_adjuster = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.sum("claimed_amount").alias("paid_total_claimed_amount"),
    )
)

rejected_by_adjuster = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.round(F.avg("claimed_amount"), 2).alias("rejected_avg_claimed_amount"),
    )
)

result = (
    paid_by_adjuster.alias("a")
    .join(rejected_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.coalesce(F.col("a.paid_total_claimed_amount"), F.lit(0)).alias("paid_total_claimed_amount"),
        F.coalesce(F.col("b.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.col("b.rejected_avg_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.rejected_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("adjuster_id")
)

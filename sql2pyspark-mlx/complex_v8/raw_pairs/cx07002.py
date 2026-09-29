from pyspark.sql import functions as F

rejected_by_adjuster = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

paid_by_adjuster = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.sum("claimed_amount").alias("paid_total_claimed_amount"),
    )
)

result = (
    rejected_by_adjuster.alias("a")
    .join(paid_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        F.coalesce(F.col("b.paid_count"), F.lit(0)).alias("paid_count"),
        F.coalesce(F.col("b.paid_total_claimed_amount"), F.lit(0)).alias("paid_total_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.paid_count"), F.lit(0))).alias("rejected_count_minus_paid_count"),
    )
    .orderBy("adjuster_id")
)

from pyspark.sql import functions as F

approved_by_adjuster = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("approved_count"),
        F.sum("claimed_amount").alias("approved_total_claimed_amount"),
    )
)

rejected_by_adjuster = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.sum("claimed_amount").alias("rejected_total_claimed_amount"),
    )
)

result = (
    approved_by_adjuster.alias("a")
    .join(rejected_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.approved_count"), F.lit(0)).alias("approved_count"),
        F.coalesce(F.col("a.approved_total_claimed_amount"), F.lit(0)).alias("approved_total_claimed_amount"),
        F.coalesce(F.col("b.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("b.rejected_total_claimed_amount"), F.lit(0)).alias("rejected_total_claimed_amount"),
        (F.coalesce(F.col("a.approved_count"), F.lit(0)) - F.coalesce(F.col("b.rejected_count"), F.lit(0))).alias("approved_count_minus_rejected_count"),
    )
    .orderBy("adjuster_id")
)

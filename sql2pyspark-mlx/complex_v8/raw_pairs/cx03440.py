from pyspark.sql import functions as F

rejected_by_adjuster = (
    df_claims
    .filter(F.col("status") == "REJECTED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("rejected_count"),
        F.round(F.avg("claimed_amount"), 2).alias("rejected_avg_claimed_amount"),
    )
)

open_by_adjuster = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("open_count"),
        F.sum("claimed_amount").alias("open_total_claimed_amount"),
    )
)

result = (
    rejected_by_adjuster.alias("a")
    .join(open_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.rejected_count"), F.lit(0)).alias("rejected_count"),
        F.coalesce(F.col("a.rejected_avg_claimed_amount"), F.lit(0)).alias("rejected_avg_claimed_amount"),
        F.coalesce(F.col("b.open_count"), F.lit(0)).alias("open_count"),
        F.coalesce(F.col("b.open_total_claimed_amount"), F.lit(0)).alias("open_total_claimed_amount"),
        (F.coalesce(F.col("a.rejected_count"), F.lit(0)) - F.coalesce(F.col("b.open_count"), F.lit(0))).alias("rejected_count_minus_open_count"),
    )
    .orderBy("adjuster_id")
)

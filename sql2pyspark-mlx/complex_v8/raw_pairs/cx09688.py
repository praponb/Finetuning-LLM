from pyspark.sql import functions as F

open_by_adjuster = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("open_count"),
        F.sum("claimed_amount").alias("open_total_claimed_amount"),
    )
)

paid_by_adjuster = (
    df_claims
    .filter(F.col("status") == "PAID")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("paid_count"),
        F.round(F.avg("claimed_amount"), 2).alias("paid_avg_claimed_amount"),
    )
)

result = (
    open_by_adjuster.alias("a")
    .join(paid_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.open_count"), F.lit(0)).alias("open_count"),
        F.col("a.open_total_claimed_amount"),
        F.coalesce(F.col("b.paid_count"), F.lit(0)).alias("paid_count"),
        F.col("b.paid_avg_claimed_amount"),
        (F.coalesce(F.col("a.open_count"), F.lit(0)) - F.coalesce(F.col("b.paid_count"), F.lit(0))).alias("open_count_minus_paid_count"),
    )
    .orderBy("adjuster_id")
)

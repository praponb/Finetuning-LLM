from pyspark.sql import functions as F

open_by_adjuster = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("open_count"),
        F.round(F.avg("claimed_amount"), 2).alias("open_avg_claimed_amount"),
    )
)

approved_by_adjuster = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("approved_count"),
        F.round(F.avg("claimed_amount"), 2).alias("approved_avg_claimed_amount"),
    )
)

result = (
    open_by_adjuster.alias("a")
    .join(approved_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.open_count"), F.lit(0)).alias("open_count"),
        F.coalesce(F.col("a.open_avg_claimed_amount"), F.lit(0)).alias("open_avg_claimed_amount"),
        F.coalesce(F.col("b.approved_count"), F.lit(0)).alias("approved_count"),
        F.col("b.approved_avg_claimed_amount"),
        (F.coalesce(F.col("a.open_count"), F.lit(0)) - F.coalesce(F.col("b.approved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("adjuster_id")
)

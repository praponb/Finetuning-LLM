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
    paid_by_adjuster.alias("a")
    .join(open_by_adjuster.alias("b"), F.col("a.adjuster_id") == F.col("b.adjuster_id"), "full")
    .select(
        F.coalesce(F.col("a.adjuster_id"), F.col("b.adjuster_id")).alias("adjuster_id"),
        F.coalesce(F.col("a.paid_count"), F.lit(0)).alias("paid_count"),
        F.coalesce(F.col("a.paid_total_claimed_amount"), F.lit(0)).alias("paid_total_claimed_amount"),
        F.coalesce(F.col("b.open_count"), F.lit(0)).alias("open_count"),
        F.coalesce(F.col("b.open_total_claimed_amount"), F.lit(0)).alias("open_total_claimed_amount"),
        (F.coalesce(F.col("a.paid_count"), F.lit(0)) - F.coalesce(F.col("b.open_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("adjuster_id")
)

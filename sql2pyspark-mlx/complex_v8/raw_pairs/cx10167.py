from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-14").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["status", "claim_status_count", "policy_id"], ["status"]],
        "status", "claim_status_count", "policy_id",
    )
    .agg(
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.max("claim_count").alias("max_claim_count"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "claim_status_count",
        "policy_id",
        "stddev_claimed_amount",
        "max_claim_count",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("claim_status_count").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

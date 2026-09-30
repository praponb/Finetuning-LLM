from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("approved_amount") >= 9389.3) & (F.col("claim_date").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    valid.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

derived = (
    with_counts
    .filter(F.col("claim_date").between(F.lit("2024-04-27").cast("date"), F.lit("2025-08-10").cast("date")))
    .select(
        "*",
        F.datediff(F.col("last_claim_date"), F.col("claim_date")).alias("days_claim_date_to_last_claim_date"),
    )
)

annotated = (
    derived
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "last_claim_date",
        "avg_claim_approved_amount",
        "days_claim_date_to_last_claim_date",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.round(F.col("approved_amount_filled") * 1.1, 2).alias("approved_amount_filled_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

result = (
    annotated
    .groupBy("policy_id", "adjuster_id")
    .pivot("claim_type", ["medical", "flood", "delay"])
    .agg(F.min("days_claim_date_to_last_claim_date"))
    .orderBy("policy_id", "adjuster_id")
)

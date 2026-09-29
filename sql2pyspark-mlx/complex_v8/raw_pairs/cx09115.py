from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("status").isin("OPEN", "APPROVED"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

scored = (
    base
    .filter(F.col("approved_amount") <= 20843.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "days_since_claim_date",
        "claimed_amount_per_approved_amount",
        F.coalesce(F.col("days_since_claim_date"), F.lit(0)).alias("days_since_claim_date_filled"),
        F.round(F.col("days_since_claim_date") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("days_since_claim_date_per_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.col("days_since_claim_date"), F.lit(0)), 3).alias("approved_amount_per_days_since_claim_date"),
    )
)

exploded_detail_claims = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.days_since_claim_date",
        "s.days_since_claim_date_filled",
        "s.approved_amount_per_days_since_claim_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    exploded_detail_claims
    .rollup("adjuster_id", "claim_status")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.min("days_since_claim_date_filled").alias("min_days_since_claim_date_filled"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_status").asc_nulls_last(),
    )
)

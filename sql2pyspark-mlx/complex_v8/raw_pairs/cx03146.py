from pyspark.sql import functions as F

recent = (
    df_claims
    .filter((F.col("claimed_amount") > 4554.8) & (F.col("adjuster_id") >= 4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_activity = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.col("c.reported_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")), "left_semi")
)

per_group = (
    with_activity
    .filter(F.col("adjuster_id").isNotNull())
    .groupBy(F.to_date("reported_at").alias("reported_at_day"), "claim_id")
    .agg(
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.round(100 * F.count(F.when(F.col("claim_status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_claim_status_is_not_null"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.count("*").alias("row_count"),
    )
)

filtered = (
    per_group.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.status") != "REJECTED") & (F.col("c.claim_date") < F.lit("2025-03-31").cast("date")), "left_anti")
)

result = (
    filtered
    .select(
        "reported_at_day",
        "claim_id",
        "median_approved_amount",
        "total_approved_amount",
        "row_count",
    )
)

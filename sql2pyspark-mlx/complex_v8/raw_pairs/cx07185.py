from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 39666.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_activity = (
    recent_claims
    .crossJoin(recent_claims.agg(F.avg("claimed_amount").alias("overall_avg_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_avg_claimed_amount"))
    .drop("overall_avg_claimed_amount")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.max("reported_at").alias("last_claim_reported_at_2"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.last_claim_reported_at_2")
)

enriched = (
    with_counts
    .select(
        "*",
        F.quarter(F.col("last_claim_reported_at_2")).alias("last_claim_reported_at_2_quarter"),
        F.to_date(F.col("last_claim_reported_at")).alias("last_claim_reported_at_date"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "id",
        "claimed_amount",
        "approved_amount_adj",
        "claimed_amount_filled",
        "approved_amount",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount_adj", "claimed_amount_filled", "approved_amount"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)

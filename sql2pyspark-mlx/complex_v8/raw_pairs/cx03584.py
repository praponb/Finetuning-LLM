from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
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
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown_2"),
    )
)

enriched = (
    base_claims
    .filter(F.col("claim_type").isin("flood", "theft"))
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj_2"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.status_or_unknown_2",
        "s.approved_amount_per_claimed_amount",
        "s.approved_amount_adj",
        "s.approved_amount_adj_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

screened = (
    detail_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-05-28").cast("date"))) & (F.col("p.end_date") >= F.lit("2026-01-23").cast("date")), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "claim_id",
        "approved_amount_adj",
        "approved_amount_per_claimed_amount",
        "approved_amount_adj_2",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["approved_amount_adj", "approved_amount_per_claimed_amount", "approved_amount_adj_2"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)

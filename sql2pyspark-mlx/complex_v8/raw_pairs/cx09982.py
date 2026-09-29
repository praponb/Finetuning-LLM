from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("policy_id") >= 9) & (F.col("reported_at") >= F.lit("2025-02-06 07:00:00").cast("timestamp")))
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
    )
)

qualified = (
    base.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("status") != "IN_REVIEW")
)

result = (
    qualified
    .select("id", "claimed_amount", "approved_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

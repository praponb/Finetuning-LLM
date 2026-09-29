from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
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
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date") >= F.lit("2025-11-20").cast("date")) & (F.col("p.product").isNotNull()), "left_semi")
)

result = (
    with_activity
    .select("id", "approved_amount", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "claimed_amount"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)

from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 19609.8)
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
        F.abs(F.col("claimed_amount") - 19337.6).alias("claimed_amount_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

filtered = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") >= 2349.3), "left_semi")
)

result = (
    filtered
    .select("id", "claimed_amount_distance", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount_distance", "claimed_amount"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)

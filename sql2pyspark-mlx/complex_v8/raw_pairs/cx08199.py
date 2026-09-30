from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claim_type") == "medical")
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

enriched = (
    cleaned_claims
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 15477.7).alias("approved_amount_distance"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount").between(2349.3, 20843.1)) & (F.col("c.claim_date") >= F.lit("2024-06-07").cast("date")), "left_semi")
    .filter(F.col("approved_amount_per_claimed_amount") >= 3.5)
)

result = (
    filtered
    .select("id", "approved_amount_distance", "approved_amount")
    .unpivot(
        ids=["id"],
        values=["approved_amount_distance", "approved_amount"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

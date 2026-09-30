from pyspark.sql import functions as F

recent = df_claims.filter(F.month(F.col("reported_at")).isin(1, 2))

scored = (
    recent
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

result = (
    scored
    .cube("status", "claim_type", "policy_id")
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.count(F.when(F.col("claim_type") == "theft", 1)).alias("n_claim_type_theft"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

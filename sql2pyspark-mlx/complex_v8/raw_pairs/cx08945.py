from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_type") != "fire") & (F.col("claim_date") < F.lit("2025-03-31").cast("date")))
    .select(F.col("policy_id").alias("entity_id"), "reported_at", "approved_amount")
)

claim_side_2 = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")) & (~F.col("claim_type").isin("flood", "fire")))
    .select(F.col("id").alias("entity_id"), "reported_at", "approved_amount")
)

result = claim_side.unionByName(claim_side_2)

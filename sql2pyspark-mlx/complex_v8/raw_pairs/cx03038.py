from pyspark.sql import functions as F

cleaned_claims = df_claims.filter(F.col("approved_amount") <= 23487.5)

claim_side = (
    cleaned_claims
    .select(F.lit("claim").alias("record_type"), "claim_type", "claimed_amount")
)

holder_side = df_policyholders.select(F.lit("holder").alias("record_type"), "region", "risk_score")

claim_rows = (
    df_claims
    .filter((F.col("adjuster_id") <= 2) & (F.col("policy_id") <= 20))
    .select(F.lit("claim_3").alias("record_type"), "status", "claimed_amount")
)

result = (
    claim_side
    .union(holder_side)
    .union(claim_rows)
    .orderBy("claim_type")
)

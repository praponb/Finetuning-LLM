from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("status").isin("PAID", "REJECTED", "APPROVED")) & (F.col("approved_amount") >= 0.0))
    .select("policy_id", "claim_date", "claim_type")
)

claim_side = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select("id", "claim_date", "status")
)

result = (
    claim_rows
    .union(claim_side)
    .distinct()
    .orderBy("policy_id", "claim_type")
)

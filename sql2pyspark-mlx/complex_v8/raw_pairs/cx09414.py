from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.col("policy_id") <= 17))
    .select("id", "reported_at", F.col("adjuster_id").alias("key_id"))
)

claim_side_2 = (
    df_claims
    .filter(F.col("status").isin("REJECTED", "IN_REVIEW", "APPROVED"))
    .select("policy_id", "reported_at", "id")
)

result = (
    claim_side
    .union(claim_side_2)
    .distinct()
)

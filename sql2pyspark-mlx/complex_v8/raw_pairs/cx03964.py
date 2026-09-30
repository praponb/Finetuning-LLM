from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (F.col("adjuster_id").isNotNull()))
    .select(F.col("policy_id").alias("key_id"), "reported_at")
)

claim_side_2 = df_claims.select("policy_id", "reported_at")

claim_rows = df_claims.select("policy_id", "reported_at")

result = (
    claim_side
    .union(claim_side_2)
    .union(claim_rows)
    .orderBy("reported_at")
)

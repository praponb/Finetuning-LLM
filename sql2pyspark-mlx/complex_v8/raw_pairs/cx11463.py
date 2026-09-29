from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("policy_id") >= 4)
    .select(
        F.col("claim_type").alias("label"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side = df_claims.select("claim_type", "reported_at")

result = (
    claim_rows
    .union(claim_side)
    .orderBy("label")
)

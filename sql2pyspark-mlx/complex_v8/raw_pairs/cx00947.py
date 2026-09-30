from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(2, 3)) & (F.col("claimed_amount") <= 43500.4))
    .select(
        F.col("adjuster_id").alias("key_id"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("status") == "APPROVED") & (F.col("claimed_amount") <= 42447.2))
    .select(
        F.col("policy_id").alias("key_id"),
        F.col("reported_at").alias("happened_at"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .orderBy(F.col("key_id").desc(), "happened_at")
)

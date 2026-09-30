from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("adjuster_id") <= 5) & (F.col("claimed_amount") <= 29336.0))
    .select(F.col("id").alias("key_id"), "claim_date", "adjuster_id")
)

claim_side_2 = (
    df_claims
    .filter(F.col("policy_id") <= 27)
    .select(
        F.col("adjuster_id").alias("key_id"),
        "claim_date",
        F.col("policy_id").alias("adjuster_id"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .orderBy("adjuster_id")
)

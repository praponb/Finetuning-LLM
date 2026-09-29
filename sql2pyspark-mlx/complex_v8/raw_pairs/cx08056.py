from pyspark.sql import functions as F

policy_rows = df_policies.select("id", "status", "premium")

claim_rows = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        F.col("policy_id").alias("id"),
        F.col("claim_type").alias("status"),
        F.col("approved_amount").alias("premium"),
    )
)

result = (
    policy_rows
    .unionByName(claim_rows)
    .distinct()
)

from pyspark.sql import functions as F

policy_side = df_policies.select("id", F.col("premium").alias("amount"), "holder_id")

claim_rows = (
    df_claims
    .filter((F.col("claimed_amount") <= 44921.3) & (F.col("policy_id").isNotNull()))
    .select(
        F.col("adjuster_id").alias("id"),
        F.col("approved_amount").alias("amount"),
        F.col("policy_id").alias("holder_id"),
    )
)

result = (
    policy_side
    .unionByName(claim_rows)
    .distinct()
    .orderBy("id", "holder_id")
)

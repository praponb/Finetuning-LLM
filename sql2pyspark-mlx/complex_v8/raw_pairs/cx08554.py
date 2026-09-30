from pyspark.sql import functions as F

claim_side = df_claims.select("id", F.col("status").alias("name"))

policy_side = df_policies.select("id", F.col("status").alias("name"))

result = (
    claim_side
    .subtract(policy_side)
    .orderBy("name", "id")
)

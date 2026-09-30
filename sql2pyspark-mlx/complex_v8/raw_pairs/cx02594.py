from pyspark.sql import functions as F

claim_rows = df_claims.select("policy_id", F.col("claim_type").alias("name"))

policy_side = df_policies.select(F.col("holder_id").alias("policy_id"), F.col("status").alias("name"))

result = (
    claim_rows
    .intersect(policy_side)
    .orderBy("name")
)

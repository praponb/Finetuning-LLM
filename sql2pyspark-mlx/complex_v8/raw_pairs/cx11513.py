from pyspark.sql import functions as F

policy_rows = df_policies.select("holder_id")

claim_side = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(F.col("adjuster_id").alias("holder_id"))
)

result = policy_rows.intersect(claim_side)

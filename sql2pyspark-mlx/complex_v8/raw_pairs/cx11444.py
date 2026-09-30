from pyspark.sql import functions as F

base_policys = df_policies.filter(F.col("holder_id").isNotNull())

policy_rows = base_policys.select(F.col("product").alias("label"))

claim_side = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select("claim_type")
)

result = policy_rows.intersect(claim_side)

from pyspark.sql import functions as F

policy_side = df_policies.select("product")

claim_side = df_claims.select("status")

result = (
    policy_side
    .intersect(claim_side)
    .orderBy(F.col("product").desc())
)

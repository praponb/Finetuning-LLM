from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("premium") >= 2013.8)
    .select(F.col("id").alias("ref_id"))
)

claim_rows = df_claims.select(F.col("id").alias("ref_id"))

result = (
    policy_side
    .subtract(claim_rows)
    .orderBy("ref_id")
)

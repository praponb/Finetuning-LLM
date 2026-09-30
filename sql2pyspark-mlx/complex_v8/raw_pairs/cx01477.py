from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(F.col("status").alias("name"))
)

policy_rows = (
    df_policies
    .filter(~F.col("product").isin("AUTO", "LIFE"))
    .select(F.col("product").alias("name"))
)

result = (
    claim_side
    .subtract(policy_rows)
    .orderBy(F.col("name").desc())
)

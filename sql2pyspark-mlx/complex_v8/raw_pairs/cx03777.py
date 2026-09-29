from pyspark.sql import functions as F

claim_side = df_claims.select("id")

policy_rows = (
    df_policies
    .filter(F.col("premium") >= 1042.8)
    .select("id")
)

result = (
    claim_side
    .exceptAll(policy_rows)
    .orderBy("id")
)

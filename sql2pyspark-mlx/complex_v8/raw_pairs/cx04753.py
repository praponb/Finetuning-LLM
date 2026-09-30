from pyspark.sql import functions as F

claim_rows = df_claims.select("adjuster_id")

claim_rows_2 = df_claims.select("policy_id")

result = (
    claim_rows
    .subtract(claim_rows_2)
    .orderBy(F.col("adjuster_id").desc())
)

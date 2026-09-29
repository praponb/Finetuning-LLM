from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .select("id", F.col("start_date").alias("day"), F.lit("policy").alias("origin"))
)

claim_rows = (
    df_claims
    .filter(F.col("approved_amount") > 4554.8)
    .select("policy_id", "claim_date", F.lit("claim").alias("origin"))
)

result = policy_rows.union(claim_rows)

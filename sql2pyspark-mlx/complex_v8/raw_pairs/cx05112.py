from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-05-07").cast("date"), F.lit("2025-03-23").cast("date")))
)

policy_rows = base.select("id")

claim_side = df_claims.select("policy_id")

result = policy_rows.subtract(claim_side)

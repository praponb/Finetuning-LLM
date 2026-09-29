from pyspark.sql import functions as F

claim_side = df_claims.select("status")

policy_rows = (
    df_policies
    .filter((F.col("premium").between(918.1, 3532.7)) & (F.col("start_date") < F.lit("2025-05-30").cast("date")))
    .select("status")
)

result = (
    claim_side
    .subtract(policy_rows)
    .orderBy(F.col("status").desc())
)

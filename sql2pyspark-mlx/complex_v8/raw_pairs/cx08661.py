from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("status").isin("REJECTED", "OPEN", "APPROVED")) & (F.col("claim_date") < F.lit("2025-01-04").cast("date")))
    .select("policy_id", F.col("approved_amount").alias("amount"))
)

policy_side = (
    df_policies
    .filter(F.col("premium") >= 1541.8)
    .select(F.col("id").alias("policy_id"), F.col("premium").alias("amount"))
)

policy_rows = (
    df_policies
    .select(F.col("holder_id").alias("policy_id"), F.col("premium").alias("amount"))
)

result = (
    claim_side
    .unionByName(policy_side)
    .unionByName(policy_rows)
    .orderBy("policy_id")
)

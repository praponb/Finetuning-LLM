from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("policy_id").alias("key_id"),
        "claim_date",
        "claimed_amount",
        F.lit("claim").alias("origin"),
    )
)

policy_side = (
    df_policies
    .filter(F.col("status") == "CANCELLED")
    .select(
        "holder_id",
        "end_date",
        "premium",
        F.lit("policy").alias("origin"),
    )
)

result = (
    claim_side
    .union(policy_side)
    .distinct()
)

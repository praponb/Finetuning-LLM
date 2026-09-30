from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2024-08-20").cast("date")) & (F.col("policy_id") >= 21))
    .select(
        F.col("adjuster_id").alias("key_id"),
        F.col("claim_date").alias("day"),
        F.lit("claim").alias("origin"),
    )
)

holder_side = (
    df_policyholders
    .select(
        F.col("id").alias("key_id"),
        F.col("joined_on").alias("day"),
        F.lit("holder").alias("origin"),
    )
)

result = (
    claim_rows
    .unionByName(holder_side)
    .distinct()
)

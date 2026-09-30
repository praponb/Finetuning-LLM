from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(F.col("id").alias("entity_id"), "policy_id", F.lit("claim").alias("src"))
)

policy_rows = (
    df_policies
    .select(
        F.col("holder_id").alias("entity_id"),
        F.col("id").alias("policy_id"),
        F.lit("policy").alias("src"),
    )
)

result = (
    claim_rows
    .unionByName(policy_rows)
    .orderBy("policy_id")
)

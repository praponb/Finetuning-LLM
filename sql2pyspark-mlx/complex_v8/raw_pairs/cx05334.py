from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(F.col("policy_id").alias("qty"), F.col("adjuster_id").alias("key_id"))
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-03-30").cast("date"), F.lit("2025-04-20").cast("date"))) & (F.col("adjuster_id").isNotNull()))
    .select(F.col("adjuster_id").alias("qty"), F.col("id").alias("key_id"))
)

policy_side = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("end_date") < F.lit("2026-06-18").cast("date")))
    .select(F.col("id").alias("qty"), F.col("holder_id").alias("key_id"))
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .unionByName(policy_side)
    .orderBy("qty", "key_id")
)

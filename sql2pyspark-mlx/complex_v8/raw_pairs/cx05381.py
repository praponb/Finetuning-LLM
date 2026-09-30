from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.col("start_date").isNotNull()))
    .select("holder_id", "id", F.lit("policy").alias("source"))
)

claim_rows = (
    df_claims
    .filter((F.col("approved_amount").between(0.0, 38909.4)) & (F.col("claim_type") == "fire"))
    .select("policy_id", "adjuster_id", F.lit("claim").alias("source"))
)

result = (
    policy_side
    .union(claim_rows)
    .distinct()
)

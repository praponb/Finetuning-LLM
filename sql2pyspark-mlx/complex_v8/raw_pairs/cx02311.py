from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp"))
    .select("policy_id", "id", F.lit("claim").alias("source"))
)

policy_side = (
    df_policies
    .filter(F.col("product").isin("LIFE", "HOME"))
    .select("id", "holder_id", F.lit("policy").alias("source"))
)

result = claim_side.union(policy_side)

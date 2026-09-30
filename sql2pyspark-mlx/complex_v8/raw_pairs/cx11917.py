from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("status") != "IN_REVIEW") & (F.col("claim_type").isin("collision", "fire", "theft")))
)

claim_rows = filtered.select(F.col("claim_type").alias("name"), "status", F.lit("claim").alias("src"))

policy_rows = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-08-27").cast("date"))
    .select("status", "product", F.lit("policy").alias("src"))
)

holder_rows = (
    df_policyholders
    .filter((F.col("region") == "Central") & (F.col("risk_score") > 0.3))
    .select("holder_name", "region", F.lit("holder").alias("src"))
)

result = (
    claim_rows
    .union(policy_rows)
    .union(holder_rows)
    .orderBy("status")
)

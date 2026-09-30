from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("G%"))
    .select(
        F.col("holder_name").alias("label"),
        F.col("risk_score").alias("metric"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select("status", "claimed_amount")
)

result = (
    holder_side
    .union(claim_side)
    .orderBy("metric", "label")
)

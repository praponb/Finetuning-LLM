from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        "segment",
        F.col("risk_score").alias("metric"),
        F.lit("holder").alias("origin"),
    )
)

claim_side = df_claims.select("claim_type", "approved_amount", F.lit("claim").alias("origin"))

result = (
    holder_side
    .union(claim_side)
    .distinct()
)

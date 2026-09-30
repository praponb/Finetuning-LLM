from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("risk_score") <= 0.7)
    .select(
        "region",
        "segment",
        F.col("risk_score").alias("metric"),
        F.lit("holder").alias("src"),
    )
)

policy_side = (
    df_policies
    .select(
        "status",
        "product",
        "premium",
        F.lit("policy").alias("src"),
    )
)

result = (
    holder_side
    .union(policy_side)
    .orderBy(F.col("region").desc(), "src")
)

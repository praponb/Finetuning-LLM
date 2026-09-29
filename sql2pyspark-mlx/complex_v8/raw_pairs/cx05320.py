from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
    )
)

claim_side = (
    cleaned
    .select(
        F.lit("claim").alias("source"),
        F.col("policy_id").alias("qty"),
        "approved_amount",
    )
)

holder_side = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("E%")) & (F.col("region").isin("Central", "North")))
    .select(
        F.lit("holder").alias("source"),
        F.col("id").alias("qty"),
        F.col("risk_score").alias("approved_amount"),
    )
)

result = claim_side.unionByName(holder_side)

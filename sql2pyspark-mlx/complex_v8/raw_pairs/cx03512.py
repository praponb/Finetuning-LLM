from pyspark.sql import functions as F

base_holders = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-04-20").cast("date")) & (F.col("region").isin("North", "South")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.when(F.col("risk_score") >= 0.6, "A").when(F.col("risk_score") >= 0.2, "B").otherwise("C").alias("risk_score_tier"),
    )
)

holder_side = (
    base_holders
    .filter(F.col("risk_score") <= 0.8)
    .select(
        F.col("region").alias("category"),
        F.col("risk_score").alias("amount"),
        F.lit("holder").alias("src"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("adjuster_id") >= 4))
    .select(
        F.col("status").alias("category"),
        F.col("claimed_amount").alias("amount"),
        F.lit("claim").alias("src"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("adjuster_id") <= 3))
    .select(
        F.col("claim_type").alias("category"),
        F.col("approved_amount").alias("amount"),
        F.lit("claim").alias("src"),
    )
)

result = (
    holder_side
    .union(claim_side)
    .union(claim_side_2)
    .orderBy("category", "amount")
)

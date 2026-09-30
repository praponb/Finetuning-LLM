from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.lit("claim").alias("record_type"),
        F.col("policy_id").alias("entity_id"),
        F.col("claimed_amount").alias("amount"),
    )
)

holder_side = (
    df_policyholders
    .filter(F.col("risk_score") < 0.8)
    .select(F.lit("holder").alias("record_type"), "id", "risk_score")
)

combined = (
    claim_side
    .union(holder_side)
    .distinct()
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("record_type")
)

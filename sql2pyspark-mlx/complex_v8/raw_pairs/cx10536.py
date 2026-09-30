from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("id").alias("entity_id"),
        F.col("claim_date").alias("day"),
        F.col("policy_id").alias("entity_id_2"),
    )
)

claim_side_2 = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-25").cast("date"))
    .select("policy_id", "claim_date", "id")
)

combined = (
    claim_side
    .union(claim_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id_2").alias("total_entity_id_2"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("entity_id")
)

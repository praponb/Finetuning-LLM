from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) & (F.col("approved_amount") <= 14993.0))
    .select(
        F.lit("claim").alias("origin"),
        F.col("policy_id").alias("entity_id"),
        "reported_at",
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("approved_amount") < 32511.8))
    .select(F.lit("claim_2").alias("origin"), "adjuster_id", "reported_at")
)

unioned = claim_side.union(claim_side_2)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("origin")
)

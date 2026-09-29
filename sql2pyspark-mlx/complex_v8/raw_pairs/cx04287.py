from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.year(F.col("reported_at")).alias("reported_at_year"),
    )
)

claim_rows = (
    base
    .select(
        F.lit("claim").alias("source"),
        "id",
        F.col("adjuster_id").alias("entity_id"),
    )
)

claim_side = (
    df_claims
    .select(
        F.lit("claim_2").alias("source"),
        F.col("policy_id").alias("id"),
        F.col("adjuster_id").alias("entity_id"),
    )
)

combined = claim_rows.unionByName(claim_side)

result = (
    combined
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("source")
)

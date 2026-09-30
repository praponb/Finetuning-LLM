from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("adjuster_id") >= 2) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

claim_rows = (
    valid
    .filter(F.upper(F.trim(F.col("status_clean"))).like("O%"))
    .select(
        F.lit("claim").alias("source"),
        F.col("policy_id").alias("key_id"),
        "reported_at",
    )
)

claim_rows_2 = (
    df_claims
    .select(
        F.lit("claim_2").alias("source"),
        F.col("id").alias("key_id"),
        "reported_at",
    )
)

combined = claim_rows.unionByName(claim_rows_2)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("source")
)

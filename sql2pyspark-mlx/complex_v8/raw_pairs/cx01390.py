from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("claimed_amount") < 30001.6))
    .select(
        F.lit("claim").alias("origin"),
        F.col("id").alias("ref_id"),
        F.col("reported_at").alias("happened_at"),
        F.col("claimed_amount").alias("metric"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_type") != "medical") & (F.col("policy_id") <= 29))
    .select(
        F.lit("claim_2").alias("origin"),
        "id",
        "reported_at",
        "approved_amount",
    )
)

combined = claim_side.union(claim_side_2)

result = (
    combined
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("metric").alias("total_metric"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("origin")
)

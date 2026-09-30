from pyspark.sql import functions as F

claim_side = df_claims.select(F.col("policy_id").alias("qty"), "reported_at")

claim_side_2 = (
    df_claims
    .filter(F.col("approved_amount") <= 20843.1)
    .select("policy_id", "reported_at")
)

claim_side_3 = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(9, 14))
    .select("adjuster_id", "reported_at")
)

stacked = (
    claim_side
    .union(claim_side_2)
    .union(claim_side_3)
)

result = (
    stacked
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("qty")
)

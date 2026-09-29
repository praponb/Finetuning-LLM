from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2024)
    .select("claim_type", F.col("id").alias("qty"), "reported_at")
)

claim_side_2 = df_claims.select("status", "policy_id", "reported_at")

combined = claim_side.union(claim_side_2)

result = (
    combined
    .groupBy("claim_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("claim_type")
)

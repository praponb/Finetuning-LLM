from pyspark.sql import functions as F

claim_side = df_claims.select(F.col("policy_id").alias("key_id"), "reported_at")

claim_rows = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("adjuster_id") >= 2))
    .select("adjuster_id", "reported_at")
)

unioned = (
    claim_side
    .union(claim_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("key_id")
)

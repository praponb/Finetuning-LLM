from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(8, 14)) & (F.col("claim_type") != "theft"))
    .select("id", F.col("reported_at").alias("happened_at"))
)

claim_side_2 = df_claims.select("adjuster_id", "reported_at")

combined = claim_side.union(claim_side_2)

result = (
    combined
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("id")
)

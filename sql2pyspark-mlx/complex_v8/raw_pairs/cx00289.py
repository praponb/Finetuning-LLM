from pyspark.sql import functions as F

claim_side = df_claims.select("claim_type", "reported_at")

claim_side_2 = (
    df_claims
    .filter((F.col("status").isin("PAID", "REJECTED")) & (F.col("adjuster_id").isNotNull()))
    .select("claim_type", "reported_at")
)

result = claim_side.unionByName(claim_side_2)

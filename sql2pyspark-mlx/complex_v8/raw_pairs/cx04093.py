from pyspark.sql import functions as F

claim_side = df_claims.select("claim_type", "reported_at", "id")

claim_side_2 = df_claims.select("status", "reported_at", "id")

result = (
    claim_side
    .union(claim_side_2)
    .orderBy(F.col("claim_type").desc())
)

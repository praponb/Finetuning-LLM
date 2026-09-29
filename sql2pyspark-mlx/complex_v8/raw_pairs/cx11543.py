from pyspark.sql import functions as F

claim_rows = df_claims.select("status", "claim_date", "reported_at")

claim_side = df_claims.select(F.col("claim_type").alias("status"), "claim_date", "reported_at")

result = (
    claim_rows
    .unionByName(claim_side)
    .distinct()
    .orderBy(F.col("status").desc())
)

from pyspark.sql import functions as F

claim_rows = df_claims.select(F.col("id").alias("entity_id"), "reported_at", "status")

claim_side = df_claims.select("policy_id", "reported_at", "status")

result = (
    claim_rows
    .union(claim_side)
    .distinct()
    .orderBy(F.col("status").desc())
)

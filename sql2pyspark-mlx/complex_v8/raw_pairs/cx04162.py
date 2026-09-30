from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")) & (F.col("claimed_amount") < 42838.9))
    .select("status", "reported_at", F.col("adjuster_id").alias("key_id"))
)

claim_side = df_claims.select("claim_type", "reported_at", "id")

result = (
    claim_rows
    .union(claim_side)
    .orderBy(F.col("reported_at").desc())
)

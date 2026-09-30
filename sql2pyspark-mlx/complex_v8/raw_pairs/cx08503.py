from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("approved_amount") <= 15477.7)
    .select(F.col("claim_type").alias("label"), "reported_at")
)

claim_side = (
    df_claims
    .filter((F.col("status") == "IN_REVIEW") & (F.col("reported_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")))
    .select("status", "reported_at")
)

claim_side_2 = df_claims.select("status", "reported_at")

result = (
    claim_rows
    .union(claim_side)
    .union(claim_side_2)
)

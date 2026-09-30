from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date")))
    .select("status", "reported_at", F.lit("claim").alias("src"))
)

claim_side = df_claims.select("claim_type", "reported_at", F.lit("claim_2").alias("src"))

result = claim_rows.union(claim_side)

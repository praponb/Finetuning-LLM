from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.year(F.col("claim_date")) == 2024))
    .select("status", "reported_at")
)

claim_rows = df_claims.select(F.col("claim_type").alias("status"), "reported_at")

result = (
    claim_side
    .unionByName(claim_rows)
    .orderBy(F.col("status").desc(), "reported_at")
)

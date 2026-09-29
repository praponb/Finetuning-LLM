from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-18").cast("date"))
    .select(F.col("claim_type").alias("name"), "reported_at")
)

claim_rows = (
    df_claims
    .filter(F.col("approved_amount") > 1273.7)
    .select(F.col("status").alias("name"), "reported_at")
)

result = (
    claim_side
    .unionByName(claim_rows)
    .orderBy("reported_at", "name")
)

from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("approved_amount") < 23120.7)
    .select(
        F.col("claim_type").alias("name"),
        "reported_at",
        F.lit("claim").alias("src"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("claimed_amount") < 29985.9)
    .select("claim_type", "reported_at", F.lit("claim_2").alias("src"))
)

result = (
    claim_rows
    .union(claim_side)
    .distinct()
)

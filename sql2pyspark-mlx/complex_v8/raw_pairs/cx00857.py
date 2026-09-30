from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) & (F.col("claimed_amount") >= 18778.6))
    .select(
        F.col("status").alias("name"),
        F.col("reported_at").alias("happened_at"),
        F.col("claim_type").alias("category"),
        F.lit("claim").alias("origin"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("claim_type").alias("name"),
        F.col("reported_at").alias("happened_at"),
        F.col("status").alias("category"),
        F.lit("claim_2").alias("origin"),
    )
)

result = (
    claim_side
    .union(claim_rows)
    .orderBy("happened_at", "origin")
)

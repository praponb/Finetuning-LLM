from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-03-31").cast("date")) & (F.col("claimed_amount") > 5774.6))
    .select(
        "claim_type",
        F.col("reported_at").alias("happened_at"),
        F.col("approved_amount").alias("value"),
        F.lit("claim").alias("src"),
    )
)

claim_rows_2 = (
    df_claims
    .select(
        "claim_type",
        "reported_at",
        "approved_amount",
        F.lit("claim_2").alias("src"),
    )
)

result = (
    claim_rows
    .union(claim_rows_2)
    .orderBy("claim_type")
)

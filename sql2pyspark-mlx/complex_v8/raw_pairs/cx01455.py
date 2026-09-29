from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("adjuster_id") <= 4) & (F.col("status") == "APPROVED"))
    .select(
        F.lit("claim").alias("origin"),
        "claim_type",
        F.col("claim_date").alias("event_date"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(F.lit("claim_2").alias("origin"), "claim_type", "claim_date")
)

result = (
    claim_rows
    .union(claim_side)
    .orderBy("claim_type")
)

from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("claim_type").alias("name"),
        F.col("claim_date").alias("event_date"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side_2 = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-09-14").cast("date"))
    .select(
        F.col("status").alias("name"),
        F.col("claim_date").alias("event_date"),
        F.col("reported_at").alias("happened_at"),
    )
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .distinct()
)

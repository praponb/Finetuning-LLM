from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("approved_amount") > 0.0))
    .select(
        F.col("claim_type").alias("category"),
        F.col("reported_at").alias("event_time"),
        F.col("claimed_amount").alias("amount"),
    )
)

claim_rows_2 = df_claims.select("claim_type", "reported_at", "approved_amount")

result = claim_rows.union(claim_rows_2)

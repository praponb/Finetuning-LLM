from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("claimed_amount") > 19337.6)
    .select(
        F.col("claim_type").alias("category"),
        F.col("reported_at").alias("event_time"),
        F.lit("claim").alias("source"),
    )
)

claim_rows_2 = df_claims.select("claim_type", "reported_at", F.lit("claim_2").alias("source"))

result = (
    claim_rows
    .union(claim_rows_2)
    .orderBy(F.col("event_time").desc(), "category")
)

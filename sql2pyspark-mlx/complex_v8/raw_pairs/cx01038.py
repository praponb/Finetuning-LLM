from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.col("status").alias("name"),
        F.col("reported_at").alias("event_time"),
        "claimed_amount",
    )
)

claim_side = df_claims.select("status", "reported_at", "claimed_amount")

claim_rows_2 = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select("status", "reported_at", "claimed_amount")
)

result = (
    claim_rows
    .union(claim_side)
    .union(claim_rows_2)
    .orderBy(F.col("event_time").desc())
)

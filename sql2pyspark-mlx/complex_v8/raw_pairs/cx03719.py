from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-06-13").cast("date"), F.lit("2025-10-29").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
    )
)

claim_rows = (
    cleaned_claims
    .select(
        F.col("policy_id").alias("key_id"),
        F.col("reported_at").alias("event_time"),
    )
)

claim_side = (
    df_claims
    .filter((~F.col("claim_type").isin("theft", "collision")) & (F.col("claimed_amount") >= 19347.2))
    .select("adjuster_id", "reported_at")
)

result = (
    claim_rows
    .union(claim_side)
    .orderBy(F.col("event_time").desc(), "key_id")
)

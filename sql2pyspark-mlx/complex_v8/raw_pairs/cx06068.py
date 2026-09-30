from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("status") != "APPROVED") & (F.col("approved_amount") >= 9389.3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

claim_side = (
    prepared
    .select(
        F.col("status").alias("category"),
        F.col("claim_date").alias("event_date"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_type").isin("medical", "theft", "fire")) & (F.col("claim_date") >= F.lit("2024-04-07").cast("date")))
    .select(
        F.col("status").alias("category"),
        F.col("claim_date").alias("event_date"),
    )
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .orderBy("category", "event_date")
)

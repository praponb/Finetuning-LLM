from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

claim_rows = (
    scoped_claims
    .filter(F.col("status") == "OPEN")
    .select(
        F.col("status").alias("label"),
        F.col("reported_at").alias("event_time"),
        F.col("claim_type").alias("label_2"),
    )
)

claim_side = df_claims.select("claim_type", "reported_at", "status")

result = (
    claim_rows
    .union(claim_side)
    .distinct()
)

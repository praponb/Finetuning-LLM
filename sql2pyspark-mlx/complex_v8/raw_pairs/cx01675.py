from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_type") == "collision")
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
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

summary = (
    cleaned
    .groupBy("status")
    .agg(
        F.min("reported_at").alias("first_reported_at"),
        F.sum("approved_amount").alias("total_approved_amount"),
    )
)

labeled = (
    summary
    .filter(~F.col("status").isin("APPROVED", "REJECTED"))
    .withColumn("status_clean", F.lower(F.trim(F.col("status"))))
)

result = (
    labeled
    .filter(F.col("first_reported_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp"))
    .select(
        "status",
        "first_reported_at",
        "total_approved_amount",
        "status_clean",
    )
    .orderBy(F.col("first_reported_at").desc())
)

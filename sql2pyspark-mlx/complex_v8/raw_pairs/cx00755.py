from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("status") == "OPEN") & (F.col("reported_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp")))
    .select(
        "claim_type",
        "reported_at",
        F.col("policy_id").alias("qty"),
        F.lit("claim").alias("origin"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_date").isNotNull()))
    .select(
        F.col("status").alias("claim_type"),
        "reported_at",
        F.col("id").alias("qty"),
        F.lit("claim_2").alias("origin"),
    )
)

result = (
    claim_rows
    .unionByName(claim_side)
    .distinct()
    .orderBy("reported_at")
)

from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        F.col("policy_id").alias("qty"),
        F.col("reported_at").alias("event_time"),
        F.col("status").alias("label"),
    )
)

claim_rows = (
    df_claims
    .filter((F.col("claim_type") == "collision") & (F.col("claimed_amount") <= 31752.9))
    .select("adjuster_id", "reported_at", "claim_type")
)

result = (
    claim_side
    .union(claim_rows)
    .orderBy("qty")
)

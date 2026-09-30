from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("policy_id") <= 17))
    .select(
        F.lit("claim").alias("origin"),
        "id",
        "status",
        "claim_date",
    )
)

claim_side = (
    df_claims
    .filter((F.col("approved_amount") > 0.0) & (F.col("claimed_amount") > 20436.5))
    .select(
        F.lit("claim_2").alias("origin"),
        "policy_id",
        "claim_type",
        "claim_date",
    )
)

all_events = (
    claim_rows
    .union(claim_side)
    .distinct()
)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("claim_date").alias("latest_claim_date"),
    )
    .orderBy("origin")
)

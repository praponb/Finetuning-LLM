from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("policy_id") <= 8) & (F.col("status") == "APPROVED"))
    .select(
        F.lit("claim").alias("source"),
        "status",
        F.col("reported_at").alias("event_time"),
        "claim_type",
    )
)

claim_side = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        F.lit("claim_2").alias("source"),
        "claim_type",
        "reported_at",
        "status",
    )
)

combined = claim_rows.union(claim_side)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)

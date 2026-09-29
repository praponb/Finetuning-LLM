from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        "status",
        F.col("reported_at").alias("happened_at"),
        F.col("claim_type").alias("label"),
    )
)

claim_rows = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_date").between(F.lit("2024-03-29").cast("date"), F.lit("2025-11-08").cast("date"))))
    .select("status", "reported_at", "claim_type")
)

combined = (
    claim_side
    .union(claim_rows)
    .distinct()
)

result = (
    combined
    .groupBy("status")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("status")
)

from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.col("claim_type") == "flood")
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
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

claim_rows = (
    filtered
    .filter(F.col("status") == "PAID")
    .select("policy_id", F.col("reported_at").alias("happened_at"), "status")
)

claim_rows_2 = (
    df_claims
    .filter((F.col("claim_date").between(F.lit("2024-04-14").cast("date"), F.lit("2025-04-17").cast("date"))) & (F.col("policy_id") <= 17))
    .select("adjuster_id", "reported_at", "claim_type")
)

claim_side = (
    df_claims
    .filter(F.col("claimed_amount").between(11235.6, 32441.2))
    .select("adjuster_id", "reported_at", "status")
)

unioned = (
    claim_rows
    .union(claim_rows_2)
    .union(claim_side)
)

result = (
    unioned
    .groupBy("status")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("policy_id").alias("total_policy_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("status")
)

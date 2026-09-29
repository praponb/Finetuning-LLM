from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id") >= 26) & (F.col("claim_date").between(F.lit("2024-06-26").cast("date"), F.lit("2025-09-14").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
    )
)

claim_side = (
    cleaned_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("adjuster_id").isNotNull()))
    .select(F.col("id").alias("qty"), "claim_date", "approved_amount")
)

claim_side_2 = df_claims.select("adjuster_id", "claim_date", "claimed_amount")

all_events = (
    claim_side
    .union(claim_side_2)
    .distinct()
)

result = (
    all_events
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("approved_amount").alias("total_approved_amount"),
        F.max("claim_date").alias("latest_claim_date"),
    )
    .orderBy("qty")
)

from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date")) & (F.col("policy_id").isNotNull()))
    .select(
        "policy_id",
        "claimed_amount",
        "adjuster_id",
        F.lit("claim").alias("src"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "claimed_amount",
        "policy_id",
        F.lit("claim_2").alias("src"),
    )
)

all_events = claim_rows.union(claim_side)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .orderBy("src")
)

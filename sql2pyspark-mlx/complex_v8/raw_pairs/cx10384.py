from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

derived = (
    base
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "days_since_claim_date",
        "approved_amount_minus_claimed_amount",
        (F.col("claimed_amount") - F.col("days_since_claim_date")).alias("claimed_amount_minus_days_since_claim_date"),
    )
)

result = (
    derived
    .groupingSets(
        [["adjuster_id", "policy_id"], ["adjuster_id"], ["policy_id"], []],
        "adjuster_id", "policy_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

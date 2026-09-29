from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-07-18").cast("date")) | (F.col("policy_id") >= 16))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.when(F.col("status").isin("OPEN", "REJECTED"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
    )
)

rollup_base = (
    base
    .filter(F.col("approved_amount_per_claimed_amount") > 2.0)
    .groupBy(F.to_date("reported_at").alias("reported_at_day"), "policy_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("claim_type_or_unknown").alias("distinct_claim_type_or_unknowns"),
        F.round(F.stddev("approved_amount_per_claimed_amount"), 2).alias("stddev_approved_amount_per_claimed_amount"),
    )
)

result = (
    rollup_base
    .filter(F.col("distinct_claim_type_or_unknowns") < 6)
    .select(
        "reported_at_day",
        "policy_id",
        "n_rows",
        "distinct_policy_ids",
        "stddev_approved_amount_per_claimed_amount",
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
    )
    .orderBy(
        F.col("stddev_approved_amount_per_claimed_amount").desc(),
        F.col("n_rows").asc(),
    )
)

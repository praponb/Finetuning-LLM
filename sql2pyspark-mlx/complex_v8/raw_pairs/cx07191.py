from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(~F.col("status").isin("PAID", "OPEN"), 1)).alias("claim_status_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_counts = (
    filtered_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.max_claim_approved_amount",
    )
)

grouped = (
    with_counts
    .filter(F.col("status").isin("PAID", "APPROVED"))
    .groupBy("adjuster_id", "policy_id")
    .agg(
        F.sum("claim_status_count").alias("total_claim_status_count"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.sum("max_claim_approved_amount").alias("sum_max_claim_approved_amount_check"),
    )
    .filter(F.col("sum_max_claim_approved_amount_check") > 12500.0)
    .drop("sum_max_claim_approved_amount_check")
)

result = (
    grouped
    .select(
        "adjuster_id",
        "distinct_adjuster_ids",
        F.abs(F.col("total_claim_status_count") - 31).alias("total_claim_status_count_distance"),
    )
    .orderBy(F.col("distinct_adjuster_ids").asc_nulls_last())
)

from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 11)) & (F.col("adjuster_id").isNotNull()))
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count("*").alias("claim_count_2"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["claim_type", "claim_count_2"], ["claim_type"], []],
        "claim_type", "claim_count_2",
    )
    .agg(
        F.grouping("claim_type").alias("is_total_claim_type"),
        F.sum(F.when(~F.col("status").isin("IN_REVIEW", "APPROVED"), F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_status_not_in_in_review_appr"),
        F.round(F.avg("avg_claim_approved_amount"), 2).alias("avg_avg_claim_approved_amount"),
        F.min("claim_count_2").alias("min_claim_count_2"),
    )
)

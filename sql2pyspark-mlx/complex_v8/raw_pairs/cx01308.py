from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2024)
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
        F.abs(F.col("claimed_amount") - 20505.7).alias("claimed_amount_distance"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status") != "REJECTED", 1)).alias("claim_status_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

summarized_rows = (
    valid_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_reported_at",
        "c.max_claim_approved_amount",
    )
)

grouped = (
    summarized_rows
    .filter(F.col("claim_type") != "fire")
    .groupBy("claim_type")
    .agg(
        F.count(F.when(~F.col("status").isin("REJECTED", "PAID"), 1)).alias("n_status_not_in_rejected_paid"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("claimed_amount_distance"), 2).alias("stddev_claimed_amount_distance"),
        F.max("claimed_amount").alias("max_claimed_amount_check"),
    )
    .filter(F.col("max_claimed_amount_check") > 10673.3)
    .drop("max_claimed_amount_check")
)

result = (
    grouped
    .groupingSets(
        [["distinct_ids", "n_status_not_in_rejected_paid"], ["distinct_ids"], []],
        "distinct_ids", "n_status_not_in_rejected_paid",
    )
    .agg(
        F.grouping("distinct_ids").alias("is_total_distinct_ids"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.count("*").alias("cnt"),
    )
)

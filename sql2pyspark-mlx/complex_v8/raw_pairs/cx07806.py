from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        F.abs(F.col("claimed_amount") - 18364.1).alias("claimed_amount_distance"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "claimed_amount_distance",
        F.when(F.col("claim_type").isin("theft", "fire"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

totals = (
    enriched
    .filter(F.col("status") == "PAID")
    .groupBy("status", "adjuster_id", "claim_type")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.count(F.when(F.col("status").isin("REJECTED", "IN_REVIEW"), 1)).alias("n_status_in_rejected_in_review"),
    )
)

enriched_2 = (
    totals
    .filter(F.col("claim_type").isin("medical", "delay", "flood"))
    .select(
        "status",
        "adjuster_id",
        "claim_type",
        "status_list",
        "distinct_claim_types",
        "n_status_in_rejected_in_review",
        (F.floor(F.col("distinct_claim_types") / 2) * 2).alias("distinct_claim_types_bucket"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        (F.floor(F.col("distinct_claim_types") / 2) * 2).alias("distinct_claim_types_bucket_2"),
    )
)

result = (
    enriched_2
    .select(
        "status",
        "adjuster_id",
        "claim_type",
        "status_list",
        "distinct_claim_types",
        "distinct_claim_types_bucket",
        "distinct_claim_types_bucket_2",
        F.upper(F.trim(F.col("status_clean"))).alias("status_clean_clean"),
        F.substring(F.trim(F.col("status_clean")), 1, 4).alias("status_clean_prefix"),
    )
    .orderBy("claim_type", "status")
)

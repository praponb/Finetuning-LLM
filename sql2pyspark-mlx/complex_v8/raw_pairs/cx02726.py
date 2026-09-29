from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")) & (F.col("status") == "APPROVED"))
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
        F.when(F.col("approved_amount") >= 19337.6, "A").when(F.col("approved_amount") >= 0.0, "B").otherwise("C").alias("approved_amount_tier"),
        F.when(F.col("approved_amount") >= 20880.4, "HIGH").when(F.col("approved_amount") >= 2569.2, "MEDIUM").otherwise("LOW").alias("approved_amount_tier_2"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

per_group = (
    qualified
    .groupBy(
        "approved_amount_tier_2",
        F.year("claim_date").alias("claim_date_year"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("status") != "REJECTED", 1)).alias("n_status_rejected"),
        F.max("approved_amount").alias("max_approved_amount"),
        F.min("claim_date").alias("first_claim_date"),
        F.sum(F.when(F.col("status").isin("IN_REVIEW", "OPEN", "PAID"), 1).otherwise(0)).alias("n_status_in_in_review_open_pai"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
)

tagged = (
    per_group
    .filter(F.size(F.col("claim_type_list")) >= 1)
    .select(
        "approved_amount_tier_2",
        "claim_date_year",
        "status",
        "n_status_rejected",
        "max_approved_amount",
        "n_status_in_in_review_open_pai",
        "claim_type_list",
        F.size(F.col("claim_type_list")).alias("claim_type_list_count"),
    )
)

result = (
    tagged
    .select(
        "approved_amount_tier_2",
        "status",
        "n_status_rejected",
        "max_approved_amount",
        "claim_type_list",
        "claim_type_list_count",
        (F.col("n_status_rejected") - F.col("claim_type_list_count")).alias("n_status_rejected_minus_claim_type_list_count"),
    )
    .orderBy("approved_amount_tier_2", "max_approved_amount")
)

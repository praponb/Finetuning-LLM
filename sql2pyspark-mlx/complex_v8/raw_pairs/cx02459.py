from pyspark.sql import functions as F

filtered = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

stats = (
    with_counts
    .filter(F.col("claim_type").isNotNull())
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
    )
    .agg(
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.count(F.when(F.col("claim_type").isin("collision", "fire", "medical"), 1)).alias("n_claim_type_in_collision_fire"),
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("n_status_open"),
        F.sum("approved_amount").alias("sum_approved_amount_check"),
    )
    .filter(F.col("sum_approved_amount_check") > 0.0)
    .drop("sum_approved_amount_check")
)

exploded = (
    stats
    .select(
        "claim_date_month",
        "policy_id",
        "total_claimed_amount",
        "n_claim_type_in_collision_fire",
        "median_approved_amount",
        "distinct_ids",
        "n_status_open",
        F.explode("claim_type_list").alias("claim_type_list_item"),
    )
)

result = (
    exploded
    .select(
        "claim_date_month",
        "policy_id",
        "n_claim_type_in_collision_fire",
        "distinct_ids",
        "n_status_open",
        "claim_type_list_item",
        F.concat(F.col("claim_type_list_item"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_list_item_ref"),
    )
    .orderBy(F.col("distinct_ids").desc())
)

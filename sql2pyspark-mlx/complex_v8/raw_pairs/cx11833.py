from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")

filtered = (
    df_claims
    .filter((F.col("status").isin("OPEN", "IN_REVIEW")) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("claimed_amount") >= 32878.5, "large").when(F.col("claimed_amount") >= 18478.3, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.min("days_since_claim_date").alias("overall_min_days_since_claim_date")))
    .filter(F.col("days_since_claim_date") > F.col("overall_min_days_since_claim_date"))
    .drop("overall_min_days_since_claim_date")
)

with_windows = (
    with_activity
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claimed_amount_tier",
        "days_since_claim_date",
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.ntile(10).over(w_all_by_approved_amount).alias("approved_amount_decile"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_approved_amount)).alias("days_since_prev_claim_date"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
    )
)

with_parent_policy = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.days_since_claim_date",
        "s.distinct_claim_type_in_policy_id",
        "s.approved_amount_decile",
        "s.rows_in_policy_id",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

policy_id_stats = (
    with_parent_policy
    .groupBy("policy_id")
    .agg(F.max("rows_in_policy_id").alias("max_rows_in_policy_id"))
)

vs_group = (
    with_parent_policy.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.rows_in_policy_id") == F.col("g.max_rows_in_policy_id"))
    .select(
        "s.*",
        "g.max_rows_in_policy_id",
        F.round(F.col("s.rows_in_policy_id") - F.col("g.max_rows_in_policy_id"), 2).alias("rows_in_policy_id_minus_max"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["approved_amount_decile", "policy_id", "distinct_claim_type_in_policy_id"], ["approved_amount_decile"], []],
        "approved_amount_decile", "policy_id", "distinct_claim_type_in_policy_id",
    )
    .agg(
        F.grouping("approved_amount_decile").alias("is_total_approved_amount_decile"),
        F.min("reported_at").alias("first_reported_at"),
        F.round(F.sum("max_rows_in_policy_id") / F.nullif(F.sum("days_since_claim_date"), F.lit(0)), 3).alias("max_rows_in_policy_id_to_days_since_claim_date_r"),
        F.round(F.avg("days_since_claim_date"), 2).alias("avg_days_since_claim_date"),
    )
)

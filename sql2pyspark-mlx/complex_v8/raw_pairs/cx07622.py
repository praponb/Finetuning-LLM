from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status_group = Window.partitionBy("policy_id", "status_group")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_policy_id_status_group_by_cl = Window.partitionBy("policy_id", "status_group").orderBy(F.col("claim_count").desc())
w_policy_id_status_group_by_cl_2 = Window.partitionBy("policy_id", "status_group").orderBy(F.col("claim_count").desc(), "id")

valid = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

filtered = (
    valid
    .crossJoin(valid.agg(F.avg("approved_amount").alias("overall_avg_approved_amount")))
    .filter(F.col("approved_amount") > F.col("overall_avg_approved_amount"))
    .drop("overall_avg_approved_amount")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("claim_status_count"),
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status") == "PAID", 1)).alias("claim_status_count_2"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled_2"),
        F.when(F.col("status").isin("IN_REVIEW", "APPROVED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.round(F.col("claim_status_count") - F.avg("claim_status_count").over(w_policy_id_status_group), 2).alias("claim_status_count_vs_policy_id_status_group_avg"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.round(F.cume_dist().over(w_policy_id_status_group_by_cl), 3).alias("claim_count_cume_dist"),
        F.rank().over(w_policy_id_status_group_by_cl_2).alias("claim_count_rank"),
        F.lag("approved_amount", 2).over(w_all_by_approved_amount).alias("prev2_approved_amount"),
        F.min("claimed_amount_filled").over(w_policy_id_status_group).alias("policy_id_status_group_min_claimed_amount_filled"),
    )
)

first_rows = with_trends.filter(F.col("claim_count_rank") == 1)

with_parent = (
    first_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.year(F.col("p.start_date")) == 2024), "inner")
    .filter(F.col("p.start_date") >= F.lit("2024-08-27").cast("date"))
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    with_parent
    .filter(F.col("claim_status_count_2") > 3)
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_hour",
        "claimed_amount_per_approved_amount",
        "claimed_amount_filled",
        "claim_status_count",
        "claim_count",
        "claim_status_count_2",
        "status_group",
        "claim_status_count_vs_policy_id_status_group_avg",
        "approved_amount_percent_rank",
        "claim_count_cume_dist",
        "claim_count_rank",
        "prev2_approved_amount",
        "policy_id_status_group_min_claimed_amount_filled",
        "policy_status",
        "policy_premium",
        F.date_add(F.col("policy_start_date"), 30).alias("policy_start_date_plus_30d"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("claim_status_count").asc_nulls_last(), "policy_premium")
)

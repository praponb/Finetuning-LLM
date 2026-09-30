from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_status_group = Window.partitionBy("status", "status_group")
w_status_group_n_status_in_rev = Window.partitionBy("status_group", "n_status_in_review").orderBy("max_approved_amount", "reported_at_date_ym", "status")

scoped_claims = (
    df_claims
    .filter(F.col("claim_type").isin("delay", "theft"))
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
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

summary = (
    scoped_claims
    .groupBy(
        F.date_format("reported_at_date", "yyyy-MM").alias("reported_at_date_ym"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status").isNotNull(), 1).otherwise(0)).alias("n_status_is_not_null"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.max("approved_amount").alias("max_approved_amount"),
        F.count(F.when(F.col("status") != "IN_REVIEW", 1)).alias("n_status_in_review"),
    )
)

labeled = (
    summary
    .filter(F.col("total_claimed_amount") >= 45085.0)
    .select(
        "*",
        F.round(F.col("total_claimed_amount") / F.nullif(F.col("n_status_in_review"), F.lit(0)), 3).alias("total_claimed_amount_per_n_status_in_review"),
        F.abs(F.col("total_claimed_amount") - 210030.0).alias("total_claimed_amount_distance"),
        F.when(F.col("status").isin("OPEN", "IN_REVIEW"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
)

with_trends = (
    labeled
    .select(
        "reported_at_date_ym",
        "status",
        "n_status_is_not_null",
        "status_list",
        "max_approved_amount",
        "n_status_in_review",
        F.round(F.col("total_claimed_amount") - F.avg("total_claimed_amount").over(w_status_status_group), 2).alias("total_claimed_amount_vs_status_status_group_avg"),
        F.dense_rank().over(w_status_group_n_status_in_rev).alias("max_approved_amount_dense_rank"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("max_approved_amount_dense_rank") <= 3) & (F.col("max_approved_amount") > 15000.0))
)

with_activity = (
    first_rows
    .crossJoin(first_rows.agg(F.avg("n_status_in_review").alias("overall_avg_n_status_in_review")))
    .filter(F.col("n_status_in_review") > F.col("overall_avg_n_status_in_review"))
    .drop("overall_avg_n_status_in_review")
)

flattened = (
    with_activity
    .select(
        "reported_at_date_ym",
        "status",
        "n_status_is_not_null",
        "max_approved_amount",
        "n_status_in_review",
        "total_claimed_amount_vs_status_status_group_avg",
        "max_approved_amount_dense_rank",
        F.explode("status_list").alias("status_list_item"),
    )
)

result = (
    flattened
    .select(
        "reported_at_date_ym",
        "status",
        "n_status_is_not_null",
        "max_approved_amount",
        "n_status_in_review",
        "total_claimed_amount_vs_status_status_group_avg",
        "max_approved_amount_dense_rank",
        "status_list_item",
        F.coalesce(F.col("n_status_in_review"), F.lit(0)).alias("n_status_in_review_filled"),
    )
    .orderBy(F.col("max_approved_amount").desc(), "n_status_in_review_filled")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_claim_type = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_claim_date_month_claim_type_2 = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type").orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_policy_id = Window.partitionBy("policy_id")
w_reported_at_month_approved_a = Window.partitionBy(F.date_trunc("month", "reported_at"), "approved_amount_rank").orderBy("prev_reported_at", "id")
w_approved_amount_rank_policy = Window.partitionBy("approved_amount_rank", "policy_id").orderBy("prev_approved_amount", "id")
w_reported_at_month_approved_a_2 = Window.partitionBy(F.date_trunc("month", "reported_at"), "approved_amount_rank").orderBy("prev_reported_at")

cleaned = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "claim_type_ref",
        "approved_amount_filled",
        F.lag("reported_at").over(w_claim_date_month_claim_type).alias("prev_reported_at"),
        F.min("approved_amount").over(w_claim_type).alias("claim_type_min_approved_amount"),
        F.last("claimed_amount").over(w_claim_date_month_claim_type_2).alias("last_claimed_amount"),
        F.sum("approved_amount").over(w_claim_type).alias("claim_type_sum_approved_amount"),
        F.lag("approved_amount").over(w_all_by_approved_amount).alias("prev_approved_amount"),
        F.rank().over(w_all_by_approved_amount_ties).alias("approved_amount_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.min("last_claimed_amount").over(w_policy_id).alias("policy_id_min_last_claimed_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_reported_at_month_approved_a)).alias("days_since_prev_claim_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_approved_amount_rank_policy)).alias("days_since_prev_claim_date_2"),
        F.round(F.cume_dist().over(w_reported_at_month_approved_a_2), 3).alias("prev_reported_at_cume_dist"),
    )
)

result = (
    metrics
    .rollup("claim_type", "policy_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("claim_type") == "fire", F.col("policy_id_min_last_claimed_amount"))).alias("policy_id_min_last_claimed_amount_claim_type_fir"),
        F.round(F.avg("days_since_prev_claim_date"), 2).alias("avg_days_since_prev_claim_date"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "policy_id",
        "gid",
        "policy_id_min_last_claimed_amount_claim_type_fir",
        "avg_days_since_prev_claim_date",
    )
)

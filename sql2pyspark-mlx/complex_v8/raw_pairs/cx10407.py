from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_to_date = Window.partitionBy(F.to_date("claim_date_month"))
w_all_by_n_status_in_approved = Window.orderBy(F.col("n_status_in_approved_paid_open").desc(), "claim_date_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_month_to_da = Window.partitionBy(F.to_date("claim_date_month_month")).orderBy(F.col("min_claimed_amount_share_of_claim_date_month_to_").desc_nulls_first(), "claim_date_month")
w_n_status_in_approved_paid_op = Window.partitionBy("n_status_in_approved_paid_open", F.month("claim_date_month_month"))

cleaned_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 34401.0) | (F.year(F.col("claim_date")) == 2024))
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
    )
)

per_group = (
    cleaned_claims
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.count(F.when(F.col("status").isin("APPROVED", "PAID", "OPEN"), 1)).alias("n_status_in_approved_paid_open"),
        F.sum("claimed_amount").alias("sum_claimed_amount_check"),
    )
    .filter(F.col("sum_claimed_amount_check") > 5313.8)
    .drop("sum_claimed_amount_check")
)

windowed = (
    per_group
    .select(
        "*",
        F.round(F.col("min_claimed_amount") / F.nullif(F.sum("min_claimed_amount").over(w_claim_date_month_to_date), F.lit(0)), 4).alias("min_claimed_amount_share_of_claim_date_month_to_"),
        F.first("min_claimed_amount").over(w_all_by_n_status_in_approved).alias("first_min_claimed_amount"),
        F.min("min_claimed_amount").over(w_claim_date_month_to_date).alias("claim_date_month_to_date_min_min_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_date_month_to_date).alias("rows_in_claim_date_month_to_date"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.avg("claim_date_month_to_date_min_min_claimed_amount").alias("overall_avg_claim_date_month_to_date_min_min_cla")))
    .filter(F.col("claim_date_month_to_date_min_min_claimed_amount") > F.col("overall_avg_claim_date_month_to_date_min_min_cla"))
    .drop("overall_avg_claim_date_month_to_date_min_min_cla")
)

tagged = (
    qualified
    .select(
        "*",
        F.coalesce(F.col("min_claimed_amount"), F.lit(0.0)).alias("min_claimed_amount_filled"),
        F.date_trunc("month", F.col("claim_date_month")).alias("claim_date_month_month"),
    )
)

with_trends = (
    tagged
    .select(
        "*",
        F.ntile(5).over(w_claim_date_month_month_to_da).alias("min_claimed_amount_share_of_claim_date_month_to__2"),
        F.min("min_claimed_amount").over(w_n_status_in_approved_paid_op).alias("n_status_in_approved_paid_open_claim_date_month_"),
        F.round(F.col("min_claimed_amount_filled") / F.nullif(F.sum("min_claimed_amount_filled").over(w_n_status_in_approved_paid_op), F.lit(0)), 4).alias("min_claimed_amount_filled_share_of_n_status_in_a"),
        F.ntile(3).over(w_claim_date_month_month_to_da).alias("min_claimed_amount_share_of_claim_date_month_to__3"),
    )
)

result = (
    with_trends
    .select(
        "claim_date_month",
        "min_claimed_amount",
        "n_status_in_approved_paid_open",
        "min_claimed_amount_share_of_claim_date_month_to_",
        "first_min_claimed_amount",
        "rows_in_claim_date_month_to_date",
        "claim_date_month_month",
        "n_status_in_approved_paid_open_claim_date_month_",
        "min_claimed_amount_share_of_claim_date_month_to__3",
    )
    .orderBy(
        "rows_in_claim_date_month_to_date",
        F.col("n_status_in_approved_paid_open").desc(),
    )
)

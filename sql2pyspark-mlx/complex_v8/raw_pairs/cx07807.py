from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_status_by_approved_amount_ru = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_approved_amoun_2 = Window.partitionBy("claim_type").orderBy("approved_amount", "id")
w_claim_date_month_by_approved = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id")
w_claim_date_month_by_approved_2 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount_filled").desc(), "id")
w_all_by_reported_at_prior_1 = Window.orderBy("reported_at", "id").rowsBetween(-1, -1)
w_claim_date_month_by_approved_3 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_by_distinct = Window.partitionBy(F.month("claim_date")).orderBy(F.col("distinct_status_in_claim_type").desc(), "id")
w_distinct_status_in_claim_typ = Window.partitionBy("distinct_status_in_claim_type", "status").orderBy("reported_at_percent_rank_per_rows_in_claim_type", "id")
w_claim_date_month_by_distinct_2 = Window.partitionBy(F.month("claim_date")).orderBy(F.col("distinct_status_in_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_status_in_claim_typ_2 = Window.partitionBy("distinct_status_in_claim_type", "status").orderBy("reported_at_percent_rank_per_rows_in_claim_type", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter(F.col("approved_amount").between(2272.7, 21750.2))
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
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

with_windows = (
    recent_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.min("claimed_amount").over(w_status_by_approved_amount_ru).alias("min_claimed_amount_so_far"),
        F.min("approved_amount_filled").over(w_claim_type).alias("claim_type_min_approved_amount_filled"),
        F.count(F.lit(1)).over(w_claim_type_by_approved_amoun).alias("running_count"),
        F.ntile(10).over(w_claim_type_by_approved_amoun_2).alias("approved_amount_decile"),
        F.min("claimed_amount").over(w_claim_date_month_by_approved).alias("min_claimed_amount_so_far_2"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.round(F.avg("approved_amount_filled").over(w_claim_type), 2).alias("claim_type_avg_approved_amount_filled"),
        F.lag("claim_date").over(w_status_by_approved_amount).alias("prev_claim_date"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_date_month_by_approved_2)).alias("approved_amount_change"),
        F.max("claimed_amount").over(w_all_by_reported_at_prior_1).alias("claimed_amount_max_1_prior"),
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.nth_value("approved_amount", 2).over(w_claim_date_month_by_approved_3).alias("second_approved_amount"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.round(F.col("reported_at_percent_rank") / F.nullif(F.col("rows_in_claim_type"), F.lit(0)), 3).alias("reported_at_percent_rank_per_rows_in_claim_type"),
        (F.floor(F.col("rows_in_claim_type") / 5) * 5).alias("rows_in_claim_type_bucket"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.row_number().over(w_claim_date_month_by_distinct).alias("rn"),
        F.lead("claim_date", 2).over(w_distinct_status_in_claim_typ).alias("next2_claim_date"),
        F.sum("rows_in_claim_type_bucket").over(w_claim_date_month_by_distinct_2).alias("running_rows_in_claim_type_bucket"),
        F.last("rows_in_claim_type", ignorenulls=True).over(w_distinct_status_in_claim_typ_2).alias("last_known_rows_in_claim_type"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "status_clean",
        "approved_amount_filled",
        "rows_in_claim_type",
        "min_claimed_amount_so_far",
        "running_count",
        "min_claimed_amount_so_far_2",
        "rows_in_policy_id",
        "reported_at_percent_rank",
        "claim_type_avg_approved_amount_filled",
        "prev_claim_date",
        "claimed_amount_max_1_prior",
        "distinct_status_in_claim_type",
        "second_approved_amount",
        "rows_in_claim_type_bucket",
        "rn",
        "running_rows_in_claim_type_bucket",
        "last_known_rows_in_claim_type",
        F.round(F.col("approved_amount_filled") / F.nullif(F.col("second_approved_amount"), F.lit(0)), 3).alias("approved_amount_filled_per_second_approved_amoun"),
    )
    .orderBy("claim_type", F.col("claim_type_avg_approved_amount_filled").asc())
)

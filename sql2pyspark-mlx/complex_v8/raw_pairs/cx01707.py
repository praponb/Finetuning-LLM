from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_approved_amount_ru = Window.partitionBy("status").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_adjuster_id_by_app = Window.partitionBy("policy_id", "adjuster_id").orderBy("approved_amount")
w_last_known_reported_at_year = Window.partitionBy(F.year("last_known_reported_at"), "status_len").orderBy("claimed_amount_vs_adjuster_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_month_by_approve = Window.partitionBy(F.month("reported_at")).orderBy(F.col("approved_amount_vs_status_avg").asc_nulls_last(), "id")
w_reported_at_month_by_approve_2 = Window.partitionBy(F.month("reported_at")).orderBy(F.col("approved_amount_vs_status_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_last_known_reported_at_year_2 = Window.partitionBy(F.year("last_known_reported_at"), "status_len").orderBy("claimed_amount_vs_adjuster_id_avg")
w_last_known_reported_at_month = Window.partitionBy(F.month("last_known_reported_at"))

base = (
    df_claims
    .filter(F.col("approved_amount") > 2569.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_windows = (
    base
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.max("claimed_amount").over(w_status_by_approved_amount_ru).alias("max_claimed_amount_so_far"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.last("reported_at", ignorenulls=True).over(w_policy_id_by_approved_amount).alias("last_known_reported_at"),
        F.max("approved_amount").over(w_policy_id_by_approved_amount).alias("max_approved_amount_so_far"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.last("claimed_amount").over(w_all_by_approved_amount_full).alias("last_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id), 2).alias("claimed_amount_vs_adjuster_id_avg"),
        F.sum("claimed_amount").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount"),
        F.round(F.cume_dist().over(w_policy_id_adjuster_id_by_app), 3).alias("approved_amount_cume_dist"),
    )
)

tagged = (
    with_windows
    .filter(F.col("max_claimed_amount_so_far") >= 100080.0)
    .select(
        "id",
        "claimed_amount",
        "status",
        "reported_at",
        "max_claimed_amount_so_far",
        "last_known_reported_at",
        "approved_amount_vs_status_avg",
        "last_claimed_amount",
        "claimed_amount_vs_adjuster_id_avg",
        "approved_amount_cume_dist",
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        (F.col("last_claimed_amount") - F.col("approved_amount")).alias("last_claimed_amount_minus_approved_amount"),
    )
)

with_trends = (
    tagged
    .select(
        "id",
        "claimed_amount",
        "status",
        "max_claimed_amount_so_far",
        "last_known_reported_at",
        "approved_amount_vs_status_avg",
        "last_claimed_amount",
        "reported_at_quarter",
        "status_len",
        F.nth_value("claimed_amount", 2).over(w_last_known_reported_at_year).alias("second_claimed_amount"),
        F.lag("reported_at_quarter", 3, 0).over(w_reported_at_month_by_approve).alias("prev3_reported_at_quarter"),
        F.nth_value("reported_at_quarter", 2).over(w_reported_at_month_by_approve_2).alias("second_reported_at_quarter"),
        F.rank().over(w_all_by_claimed_amount).alias("claimed_amount_rank"),
        F.rank().over(w_last_known_reported_at_year_2).alias("claimed_amount_vs_adjuster_id_avg_rank"),
        F.count(F.lit(1)).over(w_last_known_reported_at_month).alias("rows_in_last_known_reported_at_month"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.col("status").isin("REJECTED", "APPROVED", "PAID"))
    .select(
        "id",
        "claimed_amount",
        "max_claimed_amount_so_far",
        "last_known_reported_at",
        "approved_amount_vs_status_avg",
        "last_claimed_amount",
        "reported_at_quarter",
        "second_claimed_amount",
        "prev3_reported_at_quarter",
        "second_reported_at_quarter",
        "claimed_amount_rank",
        "claimed_amount_vs_adjuster_id_avg_rank",
        "rows_in_last_known_reported_at_month",
        "claimed_amount_cume_dist",
    )
    .orderBy("reported_at_quarter", "prev3_reported_at_quarter", "id")
    .limit(5)
)

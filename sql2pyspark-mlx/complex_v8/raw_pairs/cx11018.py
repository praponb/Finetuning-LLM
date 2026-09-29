from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_status_by_reported_at_prior = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(-6, -1)
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claimed_amoun_3 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last())
w_status_distinct_status_in_cl = Window.partitionBy("status", "distinct_status_in_claim_date_month").orderBy("claimed_amount_cume_dist", "id")
w_status = Window.partitionBy("status")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w_adjuster_id_by_claimed_amoun).alias("claimed_amount_tercile"),
        F.round(F.avg("claimed_amount").over(w_claim_date_month), 2).alias("claim_date_month_avg_claimed_amount"),
        F.max("approved_amount").over(w_status_by_reported_at_prior).alias("approved_amount_max_6_prior"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_month"),
        F.last("reported_at").over(w_adjuster_id_by_claimed_amoun_2).alias("last_reported_at"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_adjuster_id_by_claimed_amoun)).alias("approved_amount_change"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month), 2).alias("claimed_amount_vs_claim_date_month_avg"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_adjuster_id_by_claimed_amoun))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_month), 2).alias("approved_amount_vs_claim_date_month_avg"),
        F.size(F.collect_set("status").over(w_claim_date_month)).alias("distinct_status_in_claim_date_month"),
        F.min("claimed_amount").over(w_claim_date_month).alias("claim_date_month_min_claimed_amount"),
        F.dense_rank().over(w_adjuster_id_by_claimed_amoun_3).alias("claimed_amount_dense_rank"),
        F.round(F.cume_dist().over(w_adjuster_id_by_claimed_amoun), 3).alias("claimed_amount_cume_dist"),
        F.sum("claimed_amount").over(w_claim_date_month).alias("claim_date_month_sum_claimed_amount"),
        F.min("approved_amount").over(w_claim_date_month).alias("claim_date_month_min_approved_amount"),
        F.lead("reported_at").over(w_adjuster_id_by_claimed_amoun).alias("next_reported_at"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.lag("approved_amount", 2).over(w_status_distinct_status_in_cl).alias("prev2_approved_amount"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.sum("approved_amount_change").over(w_status).alias("status_sum_approved_amount_change"),
        F.nth_value("reported_at", 2).over(w_all_by_reported_at_full).alias("second_reported_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_tercile",
        "claim_date_month_avg_claimed_amount",
        "claimed_amount_share_of_claim_date_month",
        "last_reported_at",
        "approved_amount_change",
        "minutes_since_prev_reported_at",
        "approved_amount_vs_claim_date_month_avg",
        "claim_date_month_min_claimed_amount",
        "claimed_amount_dense_rank",
        "claimed_amount_cume_dist",
        "claim_date_month_sum_claimed_amount",
        "next_reported_at",
        "prev2_approved_amount",
        "status_sum_approved_amount_change",
        "second_reported_at",
        F.when(F.col("status").isin("REJECTED", "APPROVED"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
    .orderBy("status_len")
)

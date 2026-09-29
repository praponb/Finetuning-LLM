from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_claim_date_month_end = Window.orderBy(F.col("claim_date_month_end").desc(), "id")
w_policy_id_status_by_approved = Window.partitionBy("policy_id", "status").orderBy("approved_amount", "id")
w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy("claimed_amount", "id")
w_policy_id_status_by_approved_2 = Window.partitionBy("policy_id", "status").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_policy_id_status_by_approved_3 = Window.partitionBy("policy_id", "status").orderBy("approved_amount", "id").rowsBetween(-1, -1)

valid = (
    df_claims
    .filter((F.col("approved_amount") <= 25402.3) & (F.col("claim_type") != "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

windowed = (
    valid
    .select(
        "*",
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_id_by_reported_at)).alias("approved_amount_change"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
        F.round(F.cume_dist().over(w_all_by_claim_date_month_end), 3).alias("claim_date_month_end_cume_dist"),
        F.dense_rank().over(w_policy_id_status_by_approved).alias("approved_amount_dense_rank"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claim_date_month_end)).alias("claimed_amount_change"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.nth_value("claim_date", 2).over(w_policy_id_by_reported_at_ful).alias("second_claim_date"),
        F.rank().over(w_policy_id_status_by_approved).alias("approved_amount_rank"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_claime), 3).alias("claimed_amount_cume_dist"),
        F.round(F.percent_rank().over(w_policy_id_status_by_approved_2), 3).alias("approved_amount_percent_rank"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.max("claimed_amount_filled").over(w_policy_id_status_by_approved_3).alias("claimed_amount_filled_max_1_prior"),
    )
)

result = (
    windowed
    .filter(F.col("status").isin("OPEN", "APPROVED"))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "claimed_amount_filled",
        "claim_date_month_end",
        "claimed_amount_change",
        "claimed_amount_vs_policy_id_avg",
        "second_claim_date",
        "approved_amount_rank",
        "distinct_adjuster_id_in_status",
        "approved_amount_percent_rank",
        "claimed_amount_vs_status_avg",
        "claimed_amount_filled_max_1_prior",
    )
    .orderBy(
        "second_claim_date",
        F.col("claimed_amount_vs_policy_id_avg").asc_nulls_last(),
    )
)

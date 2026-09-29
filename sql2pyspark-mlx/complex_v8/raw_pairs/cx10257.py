from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_status_policy_id_by_claimed = Window.partitionBy("status", "policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-1, -1)
w_claim_type_claim_date_week_t = Window.partitionBy("claim_type", F.to_date("claim_date_week")).orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claim_date_week").desc(), "id").rowsBetween(-1, -1)
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").asc_nulls_last())
w_claim_type_claim_date_week_t_2 = Window.partitionBy("claim_type", F.to_date("claim_date_week")).orderBy("approved_amount")
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_claim_type_policy_id_by_clai_2 = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claim_date_week").desc(), "id")
w_status_policy_id_by_claimed_2 = Window.partitionBy("status", "policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-2, Window.currentRow)

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
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_status), 2).alias("claimed_amount_vs_adjuster_id_status_avg"),
        F.round(F.avg("claimed_amount").over(w_status_policy_id_by_claimed), 2).alias("claimed_amount_avg_1_prior"),
        F.sum("claimed_amount").over(w_claim_type_claim_date_week_t).alias("claimed_amount_sum_4"),
        F.round(F.avg("approved_amount_bucket").over(w_claim_type_policy_id_by_clai), 2).alias("approved_amount_bucket_avg_1_prior"),
        F.rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_rank"),
        F.round(F.percent_rank().over(w_claim_type_claim_date_week_t_2), 3).alias("approved_amount_percent_rank"),
        F.last("approved_amount").over(w_all_by_claimed_amount_full).alias("last_approved_amount"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_approved_amount"),
        F.ntile(10).over(w_all_by_claimed_amount).alias("claimed_amount_decile"),
        F.ntile(5).over(w_claim_type_policy_id_by_clai_2).alias("claim_date_week_quintile"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_status), 2).alias("approved_amount_vs_adjuster_id_status_avg"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_claimed_amount"),
        F.sum("approved_amount").over(w_status_policy_id_by_claimed_2).alias("approved_amount_sum_3"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("claimed_amount_rank") <= 2)
    .drop("claimed_amount_rank")
)

result = (
    top_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "claimed_amount_vs_adjuster_id_status_avg",
        "claimed_amount_avg_1_prior",
        "claimed_amount_sum_4",
        "approved_amount_bucket_avg_1_prior",
        "approved_amount_percent_rank",
        "adjuster_id_policy_id_avg_approved_amount",
        "claimed_amount_decile",
        "claim_date_week_quintile",
        "approved_amount_vs_adjuster_id_status_avg",
        "adjuster_id_policy_id_avg_claimed_amount",
        "approved_amount_sum_3",
        (F.floor(F.col("claimed_amount_sum_4") / 50000) * 50000).alias("claimed_amount_sum_4_bucket"),
        F.when(F.col("claimed_amount") >= 29359.4, "A").when(F.col("claimed_amount") >= 20436.5, "B").otherwise("C").alias("claimed_amount_tier"),
    )
    .orderBy(
        F.col("approved_amount_bucket_avg_1_prior").desc(),
        F.col("claimed_amount_decile").desc(),
        "id",
    )
    .limit(20)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.month("claim_date"))
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_first_known_cla = Window.partitionBy("policy_id").orderBy("first_known_claimed_amount_status_paid", "adjuster_id", "approved_amount_seq", "policy_id")
w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_seq")

scoped = (
    df_claims
    .filter(F.col("status") != "IN_REVIEW")
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
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_plus_365d",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_claim_date_month), 2).alias("approved_amount_vs_policy_id_claim_date_month_av"),
        F.sum("approved_amount").over(w_reported_at_to_date).alias("reported_at_to_date_sum_approved_amount"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.first("claimed_amount", ignorenulls=True).over(w_all_by_claim_date_running).alias("first_known_claimed_amount"),
        F.min("claimed_amount").over(w_reported_at_to_date).alias("reported_at_to_date_min_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_to_date), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_to_date"),
        F.round(F.cume_dist().over(w_policy_id_by_reported_at), 3).alias("reported_at_cume_dist"),
        F.row_number().over(w_policy_id_by_approved_amount).alias("approved_amount_seq"),
        F.size(F.collect_set("policy_id").over(w_reported_at_to_date)).alias("distinct_policy_id_in_reported_at_to_date"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id_claim_date_month), 2).alias("claimed_amount_vs_policy_id_claim_date_month_avg"),
        F.sum("approved_amount").over(w_policy_id_claim_date_month).alias("policy_id_claim_date_month_sum_approved_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date), 2).alias("claimed_amount_vs_reported_at_to_date_avg"),
        F.last("claimed_amount").over(w_policy_id_by_approved_amount_2).alias("last_claimed_amount"),
    )
)

summary = (
    ranked
    .groupBy("adjuster_id", "approved_amount_seq", "policy_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
        F.count(F.when(F.col("status") == "REJECTED", 1)).alias("n_status_rejected"),
        F.sum(F.when(F.col("status") == "IN_REVIEW", F.col("reported_at_to_date_min_claimed_amount")).otherwise(0)).alias("reported_at_to_date_min_claimed_amount_status_in"),
        F.sum(F.when(F.col("status") != "PAID", F.col("first_known_claimed_amount"))).alias("first_known_claimed_amount_status_paid"),
        F.max("claim_date_plus_365d").alias("last_claim_date_plus_365d"),
        F.round(F.sum("reported_at_to_date_sum_approved_amount") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("reported_at_to_date_sum_approved_amount_to_appro"),
    )
)

windowed = (
    summary
    .select(
        "*",
        F.row_number().over(w_policy_id_by_first_known_cla).alias("rn"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_approved_amount)).alias("distinct_policy_id_in_adjuster_id_approved_amoun"),
        F.round(F.avg("n_status_rejected").over(w_adjuster_id_approved_amount), 2).alias("adjuster_id_approved_amount_seq_avg_n_status_rej"),
        (F.col("pct_status_is_not_null") - F.lag("pct_status_is_not_null").over(w_policy_id_by_first_known_cla)).alias("pct_status_is_not_null_change"),
    )
)

result = (
    windowed
    .filter(F.col("pct_status_is_not_null") > 10.0)
    .select(
        "adjuster_id",
        "approved_amount_seq",
        "policy_id",
        "n_status_rejected",
        "reported_at_to_date_min_claimed_amount_status_in",
        "first_known_claimed_amount_status_paid",
        "last_claim_date_plus_365d",
        "reported_at_to_date_sum_approved_amount_to_appro",
        "rn",
        "distinct_policy_id_in_adjuster_id_approved_amoun",
        "adjuster_id_approved_amount_seq_avg_n_status_rej",
    )
    .orderBy(
        F.col("adjuster_id_approved_amount_seq_avg_n_status_rej").asc_nulls_last(),
        F.col("policy_id").desc(),
        "adjuster_id",
        "approved_amount_seq",
    )
    .limit(20)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_claim_date_ru = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_least_approved = Window.partitionBy("policy_id").orderBy("least_approved_amount_claimed_amount", "id").rowsBetween(-2, -1)
w_all_by_least_approved_amount = Window.orderBy("least_approved_amount_claimed_amount")
w_policy_id_by_least_approved_2 = Window.partitionBy("policy_id").orderBy("least_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_least_approved_amount_2 = Window.orderBy("least_approved_amount_claimed_amount", "id")
w_policy_id_by_least_approved_3 = Window.partitionBy("policy_id").orderBy("least_approved_amount_claimed_amount", "id")
w_status_policy_id_by_claimed = Window.partitionBy("status", "policy_id").orderBy(F.col("claimed_amount_vs_policy_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_claim_date_prior = Window.orderBy(F.col("next_claim_date").desc(), "id").rowsBetween(-1, -1)
w_all_by_next_claim_date = Window.orderBy(F.col("next_claim_date").desc(), "id")

valid = (
    df_claims
    .filter((F.col("claim_type") != "theft") | (F.col("claimed_amount") >= 4698.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.max("claimed_amount").over(w_policy_id).alias("policy_id_max_claimed_amount"),
        F.sum("claimed_amount").over(w_adjuster_id_by_claim_date_ru).alias("running_claimed_amount"),
        F.sum("claimed_amount").over(w_policy_id_by_least_approved).alias("claimed_amount_sum_2_prior"),
        F.max("approved_amount").over(w_policy_id).alias("policy_id_max_approved_amount"),
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.round(F.percent_rank().over(w_all_by_least_approved_amount), 3).alias("least_approved_amount_claimed_amount_percent_ran"),
        F.nth_value("claim_date", 2).over(w_policy_id_by_least_approved_2).alias("second_claim_date"),
        F.lag("claim_date").over(w_all_by_least_approved_amount_2).alias("prev_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.row_number().over(w_policy_id_by_least_approved_3).alias("least_approved_amount_claimed_amount_seq"),
        F.lead("claim_date").over(w_all_by_least_approved_amount_2).alias("next_claim_date"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "running_claimed_amount",
        "policy_id_max_approved_amount",
        "distinct_claim_type_in_policy_id",
        "least_approved_amount_claimed_amount_percent_ran",
        "approved_amount_vs_policy_id_avg",
        "claimed_amount_vs_policy_id_avg",
        "least_approved_amount_claimed_amount_seq",
        F.nth_value("approved_amount", 2).over(w_status_policy_id_by_claimed).alias("second_approved_amount"),
        F.max("claimed_amount_sum_2_prior").over(w_all_by_next_claim_date_prior).alias("claimed_amount_sum_2_prior_max_1_prior"),
        F.round(F.cume_dist().over(w_all_by_next_claim_date), 3).alias("next_claim_date_cume_dist"),
        (F.col("running_claimed_amount") - F.lag("running_claimed_amount").over(w_all_by_next_claim_date)).alias("running_claimed_amount_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "running_claimed_amount",
        "policy_id_max_approved_amount",
        "least_approved_amount_claimed_amount_percent_ran",
        "claimed_amount_vs_policy_id_avg",
        "least_approved_amount_claimed_amount_seq",
        "second_approved_amount",
        "claimed_amount_sum_2_prior_max_1_prior",
        "running_claimed_amount_change",
        (F.col("approved_amount") + F.col("second_approved_amount")).alias("approved_amount_plus_second_approved_amount"),
    )
    .orderBy("least_approved_amount_claimed_amount_percent_ran", "id")
    .limit(5)
)

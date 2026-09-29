from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_by_least_ap = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("least_approved_amount_claimed_amount")
w_reported_at_to_date_claim_da = Window.partitionBy(F.to_date("reported_at"), F.month("claim_date")).orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_to_date_claim_da_2 = Window.partitionBy(F.to_date("reported_at"), F.month("claim_date")).orderBy(F.col("approved_amount").desc(), "id")
w_policy_id_claim_date_month_b = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy("least_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_month_b_2 = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date")).orderBy("least_approved_amount_claimed_amount", "id")
w_claim_date_month_by_least_ap_2 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("least_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_least_approved_amount_claime = Window.partitionBy("least_approved_amount_claimed_amount_seq", "adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_least_approved_amount_claime_2 = Window.partitionBy("least_approved_amount_claimed_amount_seq", "adjuster_id").orderBy("claim_date")
w_least_approved_amount_claime_3 = Window.partitionBy("least_approved_amount_claimed_amount_seq")
w_n_claim_type_not_in_fire_the = Window.partitionBy("n_claim_type_not_in_fire_theft", "distinct_policy_ids")
w_all_by_n_claim_type_not_in_f = Window.orderBy("n_claim_type_not_in_fire_theft", "least_approved_amount_claimed_amount_rank", "second_reported_at_month", "policy_id")

filtered_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 2272.7) & (F.col("claim_date") < F.lit("2025-04-14").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

with_trends = (
    filtered_claims
    .select(
        "*",
        F.rank().over(w_claim_date_month_by_least_ap).alias("least_approved_amount_claimed_amount_rank"),
        F.last("reported_at", ignorenulls=True).over(w_reported_at_to_date_claim_da).alias("last_known_reported_at"),
        F.round(F.percent_rank().over(w_reported_at_to_date_claim_da_2), 3).alias("approved_amount_percent_rank"),
        F.first("claimed_amount", ignorenulls=True).over(w_policy_id_claim_date_month_b).alias("first_known_claimed_amount"),
        F.row_number().over(w_policy_id_claim_date_month_b_2).alias("least_approved_amount_claimed_amount_seq"),
        F.sum("claimed_amount").over(w_claim_date_month_by_least_ap_2).alias("running_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_claimed_amount"),
        F.nth_value("reported_at", 2).over(w_all_by_claimed_amount_full).alias("second_reported_at"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.first("last_known_reported_at").over(w_least_approved_amount_claime).alias("first_last_known_reported_at"),
        F.round(F.percent_rank().over(w_least_approved_amount_claime_2), 3).alias("claim_date_percent_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_least_approved_amount_claime_3), 2).alias("approved_amount_vs_least_approved_amount_claimed"),
        F.count(F.lit(1)).over(w_least_approved_amount_claime_3).alias("rows_in_least_approved_amount_claimed_amount_seq"),
    )
)

totals = (
    with_trends_2
    .groupBy(
        "least_approved_amount_claimed_amount_rank",
        F.date_trunc("month", "second_reported_at").alias("second_reported_at_month"),
        "policy_id",
    )
    .agg(
        F.round(F.sum("approved_amount_percent_rank") / F.nullif(F.sum("least_approved_amount_claimed_amount_rank"), F.lit(0)), 3).alias("approved_amount_percent_rank_to_least_approved_a"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.count(F.when(~F.col("claim_type").isin("fire", "theft"), 1)).alias("n_claim_type_not_in_fire_theft"),
        F.sum("approved_amount_vs_least_approved_amount_claimed").alias("sum_approved_amount_vs_least_approved_amount_cla"),
    )
    .filter(F.col("sum_approved_amount_vs_least_approved_amount_cla") > -25000.0)
    .drop("sum_approved_amount_vs_least_approved_amount_cla")
)

metrics = (
    totals
    .select(
        "*",
        F.max("distinct_policy_ids").over(w_n_claim_type_not_in_fire_the).alias("n_claim_type_not_in_fire_theft_distinct_policy_i"),
        F.lead("distinct_policy_ids").over(w_all_by_n_claim_type_not_in_f).alias("next_distinct_policy_ids"),
    )
)

result = (
    metrics
    .filter(F.col("n_claim_type_not_in_fire_theft_distinct_policy_i") > 11)
    .select(
        "least_approved_amount_claimed_amount_rank",
        "second_reported_at_month",
        "policy_id",
        "approved_amount_percent_rank_to_least_approved_a",
        "distinct_policy_ids",
        "n_claim_type_not_in_fire_theft",
        "n_claim_type_not_in_fire_theft_distinct_policy_i",
        "next_distinct_policy_ids",
    )
)

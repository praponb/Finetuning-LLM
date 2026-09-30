from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_status_policy_holder_id = Window.partitionBy("status", "policy_holder_id")
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_holder_id = Window.partitionBy("policy_holder_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_status_policy_id_by_last_pol = Window.partitionBy("status", "policy_id").orderBy(F.col("last_policy_end_date").desc(), "id").rowsBetween(-2, -1)
w_status_policy_id_by_last_pol_2 = Window.partitionBy("status", "policy_id").orderBy(F.col("last_policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_by_reported_at = Window.partitionBy("seq_no").orderBy("reported_at", "id")
w_adjuster_id_seq_no = Window.partitionBy("adjuster_id", "seq_no")

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

with_details = (
    valid.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") != "CANCELLED"), "inner")
    .filter(F.col("p.end_date") < F.lit("2026-06-18").cast("date"))
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.last("policy_end_date").over(w_policy_id_by_claimed_amount).alias("last_policy_end_date"),
        F.sum("claimed_amount").over(w_all_by_reported_at_running).alias("running_claimed_amount"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.min("claimed_amount").over(w_all_by_reported_at_running).alias("min_claimed_amount_so_far"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_by_claimed_amount_2)).alias("days_since_prev_claim_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_policy_holder_id), 2).alias("claimed_amount_vs_status_policy_holder_id_avg"),
        F.min("claimed_amount").over(w_all_by_claim_date_running).alias("min_claimed_amount_so_far_2"),
        F.count(F.lit(1)).over(w_policy_holder_id).alias("rows_in_policy_holder_id"),
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.lead("policy_end_date").over(w_all_by_claim_date).alias("next_policy_end_date"),
    )
)

derived = (
    windowed
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.concat(F.col("status"), F.lit("#"), F.col("seq_no").cast("string")).alias("status_ref_2"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.sum("rows_in_policy_holder_id").over(w_status_policy_id_by_last_pol).alias("rows_in_policy_holder_id_sum_2_prior"),
        F.sum("claimed_amount").over(w_status_policy_id_by_last_pol_2).alias("running_claimed_amount_2"),
        (F.col("rows_in_policy_holder_id") - F.lag("rows_in_policy_holder_id").over(w_seq_no_by_reported_at)).alias("rows_in_policy_holder_id_change"),
        F.round(F.avg("running_claimed_amount").over(w_adjuster_id_seq_no), 2).alias("adjuster_id_seq_no_avg_running_claimed_amount"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_seq_no)).alias("distinct_policy_id_in_adjuster_id_seq_no"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "policy_end_date",
        "last_policy_end_date",
        "running_claimed_amount",
        "reported_at_percent_rank",
        "min_claimed_amount_so_far",
        "days_since_prev_claim_date",
        "claimed_amount_vs_status_policy_holder_id_avg",
        "min_claimed_amount_so_far_2",
        "rows_in_policy_holder_id",
        "seq_no",
        "next_policy_end_date",
        "status_ref",
        "status_ref_2",
        "rows_in_policy_holder_id_sum_2_prior",
        "running_claimed_amount_2",
        "rows_in_policy_holder_id_change",
        "distinct_policy_id_in_adjuster_id_seq_no",
        (F.floor(F.col("running_claimed_amount") / 100000) * 100000).alias("running_claimed_amount_bucket"),
    )
    .orderBy("min_claimed_amount_so_far_2", F.col("status_ref").desc())
)

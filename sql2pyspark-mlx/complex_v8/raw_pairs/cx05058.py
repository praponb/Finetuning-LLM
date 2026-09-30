from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_claim_type = Window.partitionBy(F.year("claim_date"), "claim_type")
w_status = Window.partitionBy("status")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_claim_date_run = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_adjuster_id_claim_type_by_ap = Window.partitionBy("adjuster_id", "claim_type").orderBy("approved_amount", "id").rowsBetween(-2, -1)
w_claim_type_policy_id_by_clai_2 = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_claim_type_by_claim_date_ful = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_policy_id_by_clai_3 = Window.partitionBy("claim_type", "policy_id").orderBy("claim_date", "id").rowsBetween(-2, -1)
w_policy_id_by_approved_amount_3 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date_seq_prior = Window.orderBy(F.col("claim_date_seq").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_seq_no = Window.partitionBy("seq_no")
w_all_by_claim_date_seq_runnin = Window.orderBy(F.col("claim_date_seq").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_seq_ties = Window.orderBy(F.col("claim_date_seq").desc_nulls_first())
w_all_by_claim_date_seq_full = Window.orderBy(F.col("claim_date_seq").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_date") >= F.lit("2024-06-20").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label_2"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at_label",
        F.first("claim_date").over(w_policy_id_by_approved_amount).alias("first_claim_date"),
        F.count(F.lit(1)).over(w_claim_date_year_claim_type).alias("rows_in_claim_date_year_claim_type"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.count(F.lit(1)).over(w_claim_type_policy_id_by_clai).alias("running_count"),
        F.last("claimed_amount", ignorenulls=True).over(w_claim_type_by_claim_date_run).alias("last_known_claimed_amount"),
        F.last("claim_date", ignorenulls=True).over(w_claim_type_policy_id_by_clai).alias("last_known_claim_date"),
        F.size(F.collect_set("policy_id").over(w_status_claim_type)).alias("distinct_policy_id_in_status_claim_type"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_claim_type_by_ap), 2).alias("approved_amount_avg_2_prior"),
        F.round(F.percent_rank().over(w_claim_type_policy_id_by_clai_2), 3).alias("claim_date_percent_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status), F.lit(0)), 4).alias("approved_amount_share_of_status"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_approved_amount_2), 2).alias("approved_amount_avg_6_prior"),
        F.last("claimed_amount").over(w_claim_type_by_claim_date_ful).alias("last_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_claim_type_policy_id_by_clai_3), 2).alias("claimed_amount_avg_2_prior"),
        F.row_number().over(w_policy_id_by_approved_amount_3).alias("seq_no"),
        F.row_number().over(w_all_by_claim_date).alias("claim_date_seq"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "approved_amount",
        "status",
        "first_claim_date",
        "rows_in_claim_date_year_claim_type",
        "last_known_claimed_amount",
        "last_known_claim_date",
        "distinct_policy_id_in_status_claim_type",
        "approved_amount_avg_2_prior",
        "approved_amount_share_of_status",
        "approved_amount_avg_6_prior",
        "last_claimed_amount",
        "claimed_amount_avg_2_prior",
        F.round(F.avg("approved_amount_avg_6_prior").over(w_all_by_claim_date_seq_prior), 2).alias("approved_amount_avg_6_prior_avg_1_prior"),
        F.sum("running_count").over(w_seq_no).alias("seq_no_sum_running_count"),
        F.last("claim_date_seq", ignorenulls=True).over(w_all_by_claim_date_seq_runnin).alias("last_known_claim_date_seq"),
        F.dense_rank().over(w_all_by_claim_date_seq_ties).alias("claim_date_seq_dense_rank"),
        F.nth_value("claimed_amount_avg_2_prior", 2).over(w_all_by_claim_date_seq_full).alias("second_claimed_amount_avg_2_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "adjuster_id",
        "status",
        "first_claim_date",
        "rows_in_claim_date_year_claim_type",
        "last_known_claimed_amount",
        "last_known_claim_date",
        "distinct_policy_id_in_status_claim_type",
        "approved_amount_avg_2_prior",
        "approved_amount_share_of_status",
        "approved_amount_avg_6_prior",
        "claimed_amount_avg_2_prior",
        "approved_amount_avg_6_prior_avg_1_prior",
        "last_known_claim_date_seq",
        "claim_date_seq_dense_rank",
        "second_claimed_amount_avg_2_prior",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("adjuster_id").asc(), "id")
    .limit(20)
)

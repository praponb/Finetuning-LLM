from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status = Window.partitionBy("status")
w_claim_type_by_reported_at_mo = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_type_by_reported_at_ti = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc())
w_n_rows_n_status_in_approved = Window.partitionBy("n_rows", "n_status_in_approved_in_review")
w_all_by_min_approved_amount = Window.orderBy("min_approved_amount", "second_reported_at_day")
w_all_by_n_status_in_approved = Window.orderBy(F.col("n_status_in_approved_in_review").desc(), "second_reported_at_day").rowsBetween(-3, Window.currentRow)
w_all_by_n_status_in_approved_2 = Window.orderBy(F.col("n_status_in_approved_in_review").desc(), "second_reported_at_day")
w_second_reported_at_day_year = Window.partitionBy(F.year("second_reported_at_day"), F.year("next_second_reported_at_day"))
w_n_rows_second_reported_at_da = Window.partitionBy("n_rows", F.date_trunc("month", "second_reported_at_day")).orderBy("pct_status_in_in_review_rejected", "second_reported_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("status") == "REJECTED"))
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
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.nth_value("reported_at", 2).over(w_adjuster_id_policy_id_by_cla).alias("second_reported_at"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.round(F.avg("approved_amount_bucket").over(w_claim_type_by_reported_at_mo), 2).alias("approved_amount_bucket_avg_3"),
        F.dense_rank().over(w_claim_type_by_reported_at_ti).alias("reported_at_dense_rank"),
        F.round(F.col("approved_amount_bucket") / F.nullif(F.sum("approved_amount_bucket").over(w_status), F.lit(0)), 4).alias("approved_amount_bucket_share_of_status"),
    )
)

totals = (
    with_windows
    .groupBy(F.to_date("second_reported_at").alias("second_reported_at_day"))
    .agg(
        F.sum(F.when(F.col("status").isin("APPROVED", "IN_REVIEW"), 1).otherwise(0)).alias("n_status_in_approved_in_review"),
        F.count("*").alias("n_rows"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.round(100 * F.count(F.when(F.col("status").isin("IN_REVIEW", "REJECTED", "PAID"), 1)) / F.count("*"), 1).alias("pct_status_in_in_review_rejected"),
        F.max("claimed_amount_vs_status_avg").alias("max_claimed_amount_vs_status_avg_check"),
    )
    .filter(F.col("max_claimed_amount_vs_status_avg_check") > -15000.0)
    .drop("max_claimed_amount_vs_status_avg_check")
)

sequenced = (
    totals
    .select(
        "*",
        F.max("distinct_claim_types").over(w_n_rows_n_status_in_approved).alias("n_rows_n_status_in_approved_in_review_max_distin"),
        F.lead("second_reported_at_day").over(w_all_by_min_approved_amount).alias("next_second_reported_at_day"),
        F.round(F.avg("distinct_claim_types").over(w_n_rows_n_status_in_approved), 2).alias("n_rows_n_status_in_approved_in_review_avg_distin"),
    )
)

filtered = (
    sequenced
    .crossJoin(sequenced.agg(F.min("pct_status_in_in_review_rejected").alias("overall_min_pct_status_in_in_review_rejected")))
    .filter(F.col("pct_status_in_in_review_rejected") > F.col("overall_min_pct_status_in_in_review_rejected"))
    .drop("overall_min_pct_status_in_in_review_rejected")
)

ranked = (
    filtered
    .select(
        "*",
        F.round(F.avg("distinct_claim_types").over(w_all_by_n_status_in_approved), 2).alias("distinct_claim_types_avg_4"),
        F.row_number().over(w_all_by_n_status_in_approved_2).alias("seq_no"),
        F.round(F.avg("distinct_claim_types").over(w_second_reported_at_day_year), 2).alias("second_reported_at_day_year_next_second_reported"),
        F.sum("distinct_claim_types").over(w_n_rows_second_reported_at_da).alias("running_distinct_claim_types"),
    )
)

result = (
    ranked
    .select(
        "second_reported_at_day",
        "n_status_in_approved_in_review",
        "n_rows",
        "min_approved_amount",
        "pct_status_in_in_review_rejected",
        "n_rows_n_status_in_approved_in_review_max_distin",
        "next_second_reported_at_day",
        "n_rows_n_status_in_approved_in_review_avg_distin",
        "seq_no",
        "second_reported_at_day_year_next_second_reported",
        "running_distinct_claim_types",
    )
    .orderBy(
        F.col("running_distinct_claim_types").asc_nulls_last(),
        F.col("second_reported_at_day_year_next_second_reported").asc_nulls_last(),
        "second_reported_at_day",
    )
    .limit(20)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_reported_at_w = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at_week").desc(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_claim_type_adjuster_id_by_re = Window.partitionBy("claim_type", "adjuster_id").orderBy("reported_at", "id")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")
w_claim_type_adjuster_id_by_re_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_seq_no_by_reported_at_dense = Window.partitionBy("seq_no").orderBy(F.col("reported_at_dense_rank").desc(), "id")
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_seq_no_claim_date_month = Window.partitionBy("seq_no", F.month("claim_date"))
w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount_share_of_policy_id_adjuster_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_seq_no_by_second = Window.partitionBy("adjuster_id", "seq_no").orderBy(F.col("second_approved_amount").desc(), "id")

scoped = (
    df_claims
    .filter(F.col("status").isin("PAID", "REJECTED", "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_trunc("week", F.col("reported_at")).alias("reported_at_week"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.lead("claimed_amount", 3, 0).over(w_adjuster_id_by_reported_at_w).alias("next3_claimed_amount"),
        F.min("approved_amount").over(w_claim_type).alias("claim_type_min_approved_amount"),
        F.row_number().over(w_claim_type_adjuster_id_by_re).alias("reported_at_seq"),
        F.dense_rank().over(w_all_by_reported_at).alias("reported_at_dense_rank"),
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.nth_value("approved_amount", 2).over(w_claim_type_adjuster_id_by_re_2).alias("second_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id_adjuster_id"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at_week",
        "next3_claimed_amount",
        "reported_at_seq",
        "reported_at_dense_rank",
        "seq_no",
        "second_approved_amount",
        "claimed_amount_share_of_policy_id_adjuster_id",
        F.ntile(5).over(w_seq_no_by_reported_at_dense).alias("reported_at_dense_rank_quintile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month), 2).alias("claimed_amount_vs_claim_date_month_avg"),
        F.size(F.collect_set("adjuster_id").over(w_seq_no_claim_date_month)).alias("distinct_adjuster_id_in_seq_no_claim_date_month"),
        F.nth_value("approved_amount", 2).over(w_all_by_claimed_amount_full).alias("second_approved_amount_2"),
        F.last("seq_no").over(w_policy_id_by_claimed_amount).alias("last_seq_no"),
        F.sum("second_approved_amount").over(w_seq_no_claim_date_month).alias("seq_no_claim_date_month_sum_second_approved_amou"),
        F.nth_value("next3_claimed_amount", 2).over(w_policy_id_by_claimed_amount).alias("second_next3_claimed_amount"),
        F.round((F.unix_timestamp("reported_at_week") - F.unix_timestamp(F.lag("reported_at_week").over(w_adjuster_id_seq_no_by_second))) / 60, 1).alias("minutes_since_prev_reported_at_week"),
    )
)

result = (
    metrics
    .filter(F.col("claim_date").between(F.lit("2024-02-23").cast("date"), F.lit("2025-11-12").cast("date")))
    .select(
        "id",
        "claim_type",
        "reported_at_week",
        "reported_at_seq",
        "second_approved_amount",
        "claimed_amount_share_of_policy_id_adjuster_id",
        "reported_at_dense_rank_quintile",
        "claimed_amount_vs_claim_date_month_avg",
        "distinct_adjuster_id_in_seq_no_claim_date_month",
        "second_approved_amount_2",
        "seq_no_claim_date_month_sum_second_approved_amou",
        "minutes_since_prev_reported_at_week",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.least(F.col("next3_claimed_amount"), F.col("last_seq_no")).alias("least_next3_claimed_amount_last_seq_no"),
    )
)

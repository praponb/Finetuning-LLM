from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), "id")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_adjuster_id_reported_at_year = Window.partitionBy("adjuster_id", F.year("reported_at"))
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_or_unknown_i = Window.partitionBy("distinct_status_or_unknown_in_adjuster_id_report", "distinct_adjuster_id_in_policy_id")
w_reported_at_year_by_claim_da = Window.partitionBy(F.year("reported_at")).orderBy("claim_date", "id")
w_reported_at_year_by_claim_da_2 = Window.partitionBy(F.year("reported_at")).orderBy("claim_date", "id").rowsBetween(-3, -1)
w_reported_at_year_by_claim_da_3 = Window.partitionBy(F.year("reported_at")).orderBy("claim_date")
w_claim_date_quartile_prev2_re = Window.partitionBy("claim_date_quartile", F.date_trunc("month", "prev2_reported_at")).orderBy(F.col("policy_id_min_approved_amount").desc(), "id").rowsBetween(-3, -1)

recent = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.when(F.col("claimed_amount") >= 28900.9, "A").when(F.col("claimed_amount") >= 15381.6, "B").otherwise("C").alias("claimed_amount_tier"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_approved_amount)).alias("days_since_prev_claim_date"),
        F.ntile(4).over(w_all_by_claim_date).alias("claim_date_quartile"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.lag("approved_amount", 1, 0).over(w_adjuster_id_by_approved_amou).alias("prev_approved_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_adjuster_id_by_approved_amou)).alias("approved_amount_change"),
        F.lag("reported_at", 2).over(w_policy_id_by_approved_amount).alias("prev2_reported_at"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
        F.count(F.lit(1)).over(w_adjuster_id_reported_at_year).alias("rows_in_adjuster_id_reported_at_year"),
        F.nth_value("claim_date", 2).over(w_policy_id_by_approved_amount_2).alias("second_claim_date"),
        F.row_number().over(w_all_by_approved_amount).alias("approved_amount_seq"),
        F.size(F.collect_set("status_or_unknown").over(w_adjuster_id_reported_at_year)).alias("distinct_status_or_unknown_in_adjuster_id_report"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_reported_at_year), 2).alias("approved_amount_vs_adjuster_id_reported_at_year_"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "claimed_amount_tier",
        "days_since_prev_claim_date",
        "claim_date_quartile",
        "distinct_adjuster_id_in_policy_id",
        "approved_amount_change",
        "prev2_reported_at",
        "rows_in_adjuster_id_reported_at_year",
        "second_claim_date",
        "approved_amount_seq",
        "approved_amount_vs_adjuster_id_reported_at_year_",
        F.round(F.col("prev_approved_amount") / F.nullif(F.sum("prev_approved_amount").over(w_distinct_status_or_unknown_i), F.lit(0)), 4).alias("prev_approved_amount_share_of_distinct_status_or"),
        F.lead("claim_date_quartile").over(w_reported_at_year_by_claim_da).alias("next_claim_date_quartile"),
        F.size(F.collect_set("claimed_amount_tier").over(w_distinct_status_or_unknown_i)).alias("distinct_claimed_amount_tier_in_distinct_status_"),
        F.sum("claim_date_quartile").over(w_distinct_status_or_unknown_i).alias("distinct_status_or_unknown_in_adjuster_id_report_2"),
        F.round(F.avg("prev_approved_amount").over(w_reported_at_year_by_claim_da_2), 2).alias("prev_approved_amount_avg_3_prior"),
        F.round(F.cume_dist().over(w_reported_at_year_by_claim_da_3), 3).alias("claim_date_cume_dist"),
        F.sum("approved_amount").over(w_claim_date_quartile_prev2_re).alias("approved_amount_sum_3_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("prev_approved_amount_share_of_distinct_status_or") < 0.6)
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "reported_at",
        "days_since_prev_claim_date",
        "claim_date_quartile",
        "distinct_adjuster_id_in_policy_id",
        "approved_amount_change",
        "prev2_reported_at",
        "rows_in_adjuster_id_reported_at_year",
        "second_claim_date",
        "approved_amount_seq",
        "approved_amount_vs_adjuster_id_reported_at_year_",
        "prev_approved_amount_share_of_distinct_status_or",
        "distinct_status_or_unknown_in_adjuster_id_report_2",
        "prev_approved_amount_avg_3_prior",
        "claim_date_cume_dist",
        "approved_amount_sum_3_prior",
        F.date_add(F.col("second_claim_date"), 30).alias("second_claim_date_plus_30d"),
    )
    .orderBy(F.col("approved_amount_seq").desc_nulls_first())
)

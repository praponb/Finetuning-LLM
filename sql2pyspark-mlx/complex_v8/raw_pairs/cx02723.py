from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id_by_app = Window.partitionBy("adjuster_id", "policy_id").orderBy("approved_amount", "id")
w_claim_date_year_approved_amo = Window.partitionBy(F.year("claim_date"), "approved_amount_seq").orderBy(F.col("approved_amount_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_adjuster_id = Window.partitionBy("rn", "adjuster_id")
w_claim_date_year_by_days_sinc = Window.partitionBy(F.year("claim_date")).orderBy(F.col("days_since_prev_claim_date").desc())
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount_avg_3", "id").rowsBetween(-3, Window.currentRow)

cleaned_claims = (
    df_claims
    .filter(F.col("status").isin("REJECTED", "OPEN", "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

windowed = (
    cleaned_claims
    .select(
        "*",
        F.row_number().over(w_policy_id_by_approved_amount).alias("approved_amount_seq"),
        F.sum("claimed_amount").over(w_policy_id_by_approved_amount_2).alias("claimed_amount_sum_3"),
        F.row_number().over(w_policy_id_adjuster_id_by_cla).alias("rn"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.nth_value("claim_date", 2).over(w_policy_id_adjuster_id_by_cla_2).alias("second_claim_date"),
        F.nth_value("claimed_amount", 2).over(w_policy_id_adjuster_id_by_cla_2).alias("second_claimed_amount"),
        F.row_number().over(w_adjuster_id_policy_id_by_app).alias("seq_no"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_adjuster_id_by_cla)).alias("days_since_prev_claim_date"),
        F.round(F.avg("claimed_amount").over(w_policy_id_by_approved_amount_2), 2).alias("claimed_amount_avg_3"),
    )
)

top_rows = windowed.filter(F.col("rn") <= 5)

metrics = (
    top_rows
    .select(
        "*",
        F.first("claim_date", ignorenulls=True).over(w_claim_date_year_approved_amo).alias("first_known_claim_date"),
        F.round(F.avg("approved_amount_seq").over(w_rn_adjuster_id), 2).alias("rn_adjuster_id_avg_approved_amount_seq"),
        F.sum("approved_amount").over(w_rn_adjuster_id).alias("rn_adjuster_id_sum_approved_amount"),
        F.round(F.cume_dist().over(w_claim_date_year_by_days_sinc), 3).alias("days_since_prev_claim_date_cume_dist"),
        F.round(F.col("days_since_prev_claim_date") - F.avg("days_since_prev_claim_date").over(w_rn_adjuster_id), 2).alias("days_since_prev_claim_date_vs_rn_adjuster_id_avg"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_rn_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_rn_adjuster_id"),
        F.max("seq_no").over(w_adjuster_id_by_claimed_amoun).alias("seq_no_max_4"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.date_trunc("month", "first_known_claim_date").alias("first_known_claim_date_month"),
    )
    .agg(
        F.round(F.avg("days_since_prev_claim_date_vs_rn_adjuster_id_avg"), 2).alias("avg_days_since_prev_claim_date_vs_rn_adjuster_id"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount_share_of_rn_adjuster_id"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_share_of_rn_ad"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.avg("claimed_amount_avg_3").alias("avg_claimed_amount_avg_3_check"),
    )
    .filter(F.col("avg_claimed_amount_avg_3_check") > 15070.0)
    .drop("avg_claimed_amount_avg_3_check")
)

result = (
    per_group
    .filter(F.col("distinct_adjuster_ids") >= 4)
    .select(
        "first_known_claim_date_month",
        "distinct_adjuster_ids",
        F.when(F.col("distinct_adjuster_ids") >= 6, "A").when(F.col("distinct_adjuster_ids") >= 3, "B").otherwise("C").alias("distinct_adjuster_ids_tier"),
        (F.floor(F.col("distinct_adjuster_ids") / 2) * 2).alias("distinct_adjuster_ids_bucket"),
    )
    .orderBy(F.col("distinct_adjuster_ids_tier").desc())
)

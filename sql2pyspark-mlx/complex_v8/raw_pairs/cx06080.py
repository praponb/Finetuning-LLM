from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status_or_unknown = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id").rowsBetween(-1, -1)
w_policy_id_status_or_unknown_2 = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_holder_id_status_or_u = Window.partitionBy("policy_holder_id", "status_or_unknown")
w_policy_id_status_or_unknown_3 = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id").rowsBetween(-6, -1)
w_policy_id_status_or_unknown_4 = Window.partitionBy("policy_id", "status_or_unknown").orderBy("claim_date", "id")
w_policy_status_claim_date_yea = Window.partitionBy("policy_status", F.year("claim_date"))
w_all_by_policy_start_date = Window.orderBy(F.col("policy_start_date").desc_nulls_first(), "id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_vs_policy_holder_id_status_or_unk").desc(), "id")
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_vs_policy_holder_id_status_or_unk").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_start_date_month_clai = Window.partitionBy(F.month("policy_start_date"), F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_policy_start_date_month_clai_2 = Window.partitionBy(F.month("policy_start_date"), F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_start_date_month_clai_3 = Window.partitionBy(F.month("policy_start_date"), F.date_trunc("month", "claim_date")).orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

scoped_claims = (
    df_claims
    .filter(F.col("status") != "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_parent_policy = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

metrics = (
    with_parent_policy
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_policy_id_status_or_unknown), 2).alias("claimed_amount_avg_1_prior"),
        F.sum("approved_amount").over(w_policy_id_status_or_unknown_2).alias("running_approved_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_holder_id_status_or_u), 2).alias("claimed_amount_vs_policy_holder_id_status_or_unk"),
        F.round(F.avg("approved_amount").over(w_policy_id_status_or_unknown_3), 2).alias("approved_amount_avg_6_prior"),
        F.row_number().over(w_policy_id_status_or_unknown_4).alias("claim_date_seq"),
        F.sum("approved_amount").over(w_policy_status_claim_date_yea).alias("policy_status_claim_date_year_sum_approved_amoun"),
        F.ntile(10).over(w_all_by_policy_start_date).alias("policy_start_date_decile"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_holder_id_status_or_u), F.lit(0)), 4).alias("approved_amount_share_of_policy_holder_id_status"),
        F.lag("approved_amount", 3).over(w_policy_id_status_or_unknown_4).alias("prev3_approved_amount"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.lead("approved_amount", 2).over(w_adjuster_id_by_claimed_amoun).alias("next2_approved_amount"),
        F.nth_value("approved_amount", 2).over(w_adjuster_id_by_claimed_amoun_2).alias("second_approved_amount"),
        (F.col("prev3_approved_amount") - F.lag("prev3_approved_amount").over(w_policy_start_date_month_clai)).alias("prev3_approved_amount_change"),
        F.nth_value("claimed_amount_avg_1_prior", 2).over(w_policy_start_date_month_clai_2).alias("second_claimed_amount_avg_1_prior"),
        F.round(F.col("claimed_amount_avg_1_prior") - F.avg("claimed_amount_avg_1_prior").over(w_adjuster_id_policy_id), 2).alias("claimed_amount_avg_1_prior_vs_adjuster_id_policy"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_policy_start_date_month_clai)).alias("approved_amount_change"),
        F.round(F.avg("claimed_amount_avg_1_prior").over(w_policy_start_date_month_clai_3), 2).alias("claimed_amount_avg_1_prior_avg_4"),
        F.round(F.percent_rank().over(w_adjuster_id_by_claimed_amoun), 3).alias("claimed_amount_vs_policy_holder_id_status_or_unk_2"),
    )
)

result = (
    windowed
    .filter(F.col("status_or_unknown").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "status_or_unknown",
        "policy_holder_id",
        "policy_start_date",
        "policy_status_claim_date_year_sum_approved_amoun",
        "policy_start_date_decile",
        "approved_amount_share_of_policy_holder_id_status",
        "prev3_approved_amount",
        "next2_approved_amount",
        "second_approved_amount",
        "prev3_approved_amount_change",
        "approved_amount_change",
        "claimed_amount_avg_1_prior_avg_4",
        F.datediff(F.col("claim_date"), F.col("policy_start_date")).alias("days_policy_start_date_to_claim_date"),
        F.least(F.col("claim_date_seq"), F.col("next2_approved_amount")).alias("least_claim_date_seq_next2_approved_amount"),
    )
    .orderBy(
        "least_claim_date_seq_next2_approved_amount",
        F.col("claimed_amount").desc(),
    )
)

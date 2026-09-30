from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_status_or_unknown_claim_type = Window.partitionBy("status_or_unknown", "claim_type").orderBy(F.col("claim_date").desc())
w_adjuster_id = Window.partitionBy("adjuster_id")
w_reported_at_to_date_policy_i = Window.partitionBy(F.to_date("reported_at"), "policy_id").orderBy("reported_at", "id")
w_adjuster_id_reported_at_mont = Window.partitionBy("adjuster_id", F.date_trunc("month", "reported_at"))
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_approved_amoun_2 = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc())
w_claim_type_by_approved_amoun_3 = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_reported_at_to_date_policy_i_2 = Window.partitionBy(F.to_date("reported_at"), "policy_id").orderBy("reported_at", "id").rowsBetween(-3, Window.currentRow)

scoped = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.max("approved_amount").over(w_claim_type_by_approved_amoun).alias("approved_amount_max_3"),
        F.round(F.percent_rank().over(w_status_or_unknown_claim_type), 3).alias("claim_date_percent_rank"),
        F.sum("approved_amount").over(w_adjuster_id).alias("adjuster_id_sum_approved_amount"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.dense_rank().over(w_reported_at_to_date_policy_i).alias("reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_adjuster_id_reported_at_mont).alias("rows_in_adjuster_id_reported_at_month"),
        F.nth_value("approved_amount", 2).over(w_all_by_reported_at_full).alias("second_approved_amount"),
        F.sum("approved_amount").over(w_adjuster_id_reported_at_mont).alias("adjuster_id_reported_at_month_sum_approved_amoun"),
        F.dense_rank().over(w_claim_type_by_approved_amoun_2).alias("approved_amount_dense_rank"),
        F.max("approved_amount").over(w_adjuster_id_reported_at_mont).alias("adjuster_id_reported_at_month_max_approved_amoun"),
        F.nth_value("claim_date", 2).over(w_claim_type_by_approved_amoun_3).alias("second_claim_date"),
        F.round(F.percent_rank().over(w_all_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.first("approved_amount").over(w_claim_type_by_approved_amoun_3).alias("first_approved_amount"),
        F.round(F.avg("approved_amount").over(w_reported_at_to_date_policy_i_2), 2).alias("approved_amount_avg_4"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        "status_or_unknown",
        "approved_amount_vs_claim_type_avg",
        "claim_date_percent_rank",
        "rows_in_claim_type",
        "reported_at_dense_rank",
        "rows_in_adjuster_id_reported_at_month",
        "second_approved_amount",
        "adjuster_id_reported_at_month_sum_approved_amoun",
        "approved_amount_dense_rank",
        "adjuster_id_reported_at_month_max_approved_amoun",
        "second_claim_date",
        "reported_at_percent_rank",
        "first_approved_amount",
        "approved_amount_avg_4",
        F.date_trunc("quarter", F.col("second_claim_date")).alias("second_claim_date_quarter"),
    )
    .orderBy(
        F.col("second_approved_amount").asc_nulls_last(),
        F.col("rows_in_adjuster_id_reported_at_month").desc(),
        "id",
    )
    .limit(20)
)

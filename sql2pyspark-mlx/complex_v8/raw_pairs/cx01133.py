from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_prior = Window.orderBy("claimed_amount", "id").rowsBetween(-6, -1)
w_adjuster_id_policy_id_by_app = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_policy_id_status_by_claimed = Window.partitionBy("policy_id", "status").orderBy(F.col("claimed_amount_filled").desc_nulls_first(), "id")
w_policy_id_status_by_claimed_2 = Window.partitionBy("policy_id", "status").orderBy(F.col("claimed_amount_filled").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_adjuster_id_policy_id_by_cla_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(-3, -1)
w_adjuster_id_policy_id_by_cla_3 = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id")
w_status = Window.partitionBy("status")
w_policy_id_by_reported_at_plu = Window.partitionBy("policy_id").orderBy(F.col("reported_at_plus_2h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "status",
        "claimed_amount_filled",
        "reported_at_plus_2h",
        F.sum("approved_amount").over(w_claim_type).alias("claim_type_sum_approved_amount"),
        F.max("claimed_amount_filled").over(w_adjuster_id_policy_id_by_cla).alias("max_claimed_amount_filled_so_far"),
        F.max("claimed_amount").over(w_all_by_claimed_amount_prior).alias("claimed_amount_max_6_prior"),
        F.row_number().over(w_adjuster_id_policy_id_by_app).alias("seq_no"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.round(F.percent_rank().over(w_policy_id_status_by_claimed), 3).alias("claimed_amount_filled_percent_rank"),
        F.sum("claimed_amount").over(w_policy_id_status_by_claimed_2).alias("running_claimed_amount"),
        F.max("claimed_amount_filled").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_max_claimed_amount_filled"),
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_policy_id_by_cla_2), 2).alias("claimed_amount_avg_3_prior"),
        F.round(F.percent_rank().over(w_adjuster_id_policy_id_by_app), 3).alias("approved_amount_percent_rank"),
        F.lag("claimed_amount").over(w_adjuster_id_policy_id_by_cla_3).alias("prev_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("claimed_amount_dense_rank") <= 2)
    .drop("claimed_amount_dense_rank")
)

sequenced_2 = (
    top_per_group
    .select(
        "id",
        "approved_amount",
        "status",
        "claimed_amount_filled",
        "claim_type_sum_approved_amount",
        "max_claimed_amount_filled_so_far",
        "claimed_amount_max_6_prior",
        "seq_no",
        "claimed_amount_filled_percent_rank",
        "running_claimed_amount",
        "adjuster_id_claim_type_max_claimed_amount_filled",
        "approved_amount_percent_rank",
        "prev_claimed_amount",
        "claimed_amount_vs_claim_type_avg",
        F.size(F.collect_set("policy_id").over(w_status)).alias("distinct_policy_id_in_status"),
        F.max("prev_claimed_amount").over(w_status).alias("status_max_prev_claimed_amount"),
        F.sum("claimed_amount_avg_3_prior").over(w_policy_id_by_reported_at_plu).alias("running_claimed_amount_avg_3_prior"),
        F.min("approved_amount_percent_rank").over(w_policy_id_by_reported_at_plu).alias("min_approved_amount_percent_rank_so_far"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "approved_amount",
        "claimed_amount_filled",
        "max_claimed_amount_filled_so_far",
        "claimed_amount_filled_percent_rank",
        "running_claimed_amount",
        "adjuster_id_claim_type_max_claimed_amount_filled",
        "approved_amount_percent_rank",
        "claimed_amount_vs_claim_type_avg",
        "status_max_prev_claimed_amount",
    )
    .orderBy("claimed_amount_vs_claim_type_avg", "approved_amount")
)

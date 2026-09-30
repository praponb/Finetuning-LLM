from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_claim_type_by_reported_at_ti = Window.partitionBy("claim_type").orderBy("reported_at")
w_claim_date_year_by_reported = Window.partitionBy(F.year("claim_date")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_claim_date_year_by_reported_2 = Window.partitionBy(F.year("claim_date")).orderBy("reported_at", "id")
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_claim_type_reported_at_dense = Window.partitionBy("claim_type", "reported_at_dense_rank").orderBy(F.col("reported_at_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_claim_type_reported_at_dense_2 = Window.partitionBy("claim_type", "reported_at_dense_rank").orderBy(F.col("reported_at_cume_dist").desc(), "id")
w_claim_type_reported_at_dense_3 = Window.partitionBy("claim_type", "reported_at_dense_rank").orderBy(F.col("reported_at_cume_dist").desc())

valid = (
    df_claims
    .filter((F.col("approved_amount") >= 4234.9) & (F.col("claim_date") < F.lit("2025-05-26").cast("date")))
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
        F.round(F.cume_dist().over(w_claim_type_by_reported_at_ti), 3).alias("reported_at_cume_dist"),
        F.nth_value("claimed_amount", 2).over(w_claim_date_year_by_reported).alias("second_claimed_amount"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.lead("reported_at").over(w_claim_date_year_by_reported_2).alias("next_reported_at"),
        F.count(F.lit(1)).over(w_policy_id_status).alias("rows_in_policy_id_status"),
        F.dense_rank().over(w_claim_type_by_reported_at_ti).alias("reported_at_dense_rank"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount_share_of_claim_type",
        "second_claimed_amount",
        "next_reported_at",
        F.sum("reported_at_dense_rank").over(w_claim_type_reported_at_dense).alias("running_reported_at_dense_rank"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id_policy_id)).alias("distinct_claim_type_in_adjuster_id_policy_id"),
        F.round(F.col("rows_in_policy_id_status") - F.avg("rows_in_policy_id_status").over(w_adjuster_id_policy_id), 2).alias("rows_in_policy_id_status_vs_adjuster_id_policy_i"),
        F.rank().over(w_claim_type_reported_at_dense_2).alias("reported_at_cume_dist_rank"),
        F.min("reported_at_cume_dist").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_min_reported_at_cume_dist"),
        F.round(F.cume_dist().over(w_claim_type_reported_at_dense_3), 3).alias("reported_at_cume_dist_cume_dist"),
        F.round(F.col("second_claimed_amount") - F.avg("second_claimed_amount").over(w_adjuster_id_policy_id), 2).alias("second_claimed_amount_vs_adjuster_id_policy_id_a"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount_share_of_claim_type",
        "second_claimed_amount",
        "next_reported_at",
        "running_reported_at_dense_rank",
        "distinct_claim_type_in_adjuster_id_policy_id",
        "reported_at_cume_dist_rank",
        "adjuster_id_policy_id_min_reported_at_cume_dist",
        "reported_at_cume_dist_cume_dist",
        "second_claimed_amount_vs_adjuster_id_policy_id_a",
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
    .orderBy(
        F.col("adjuster_id_policy_id_min_reported_at_cume_dist").desc_nulls_first(),
        F.col("approved_amount_share_of_claim_type").asc_nulls_last(),
    )
)

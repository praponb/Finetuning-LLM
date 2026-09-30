from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_approved_amount_full = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_distinct_status_in_cl = Window.orderBy(F.col("distinct_status_in_claim_type").desc(), "id")
w_reported_at_quintile_by_seco = Window.partitionBy("reported_at_quintile").orderBy(F.col("second_approved_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_approved_amount_dense_rank_s = Window.partitionBy("approved_amount_dense_rank", "status")

valid = (
    df_claims
    .filter(F.col("policy_id") <= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_adjuster_id"),
        F.ntile(5).over(w_all_by_reported_at).alias("reported_at_quintile"),
        F.max("approved_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_max_approved_amount"),
        F.min("approved_amount").over(w_all_by_reported_at_running).alias("min_approved_amount_so_far"),
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.dense_rank().over(w_all_by_reported_at).alias("reported_at_dense_rank"),
        F.nth_value("approved_amount", 2).over(w_all_by_approved_amount_full).alias("second_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.max("approved_amount").over(w_claim_type).alias("claim_type_max_approved_amount"),
        F.last("claimed_amount").over(w_all_by_reported_at_full).alias("last_claimed_amount"),
        F.dense_rank().over(w_all_by_approved_amount).alias("approved_amount_dense_rank"),
    )
)

top_rows = (
    metrics
    .filter(F.col("reported_at_dense_rank") <= 2)
    .drop("reported_at_dense_rank")
)

metrics_2 = (
    top_rows
    .select(
        "id",
        "approved_amount",
        "status",
        "approved_amount_share_of_claim_type_adjuster_id",
        "reported_at_quintile",
        "claim_type_adjuster_id_max_approved_amount",
        "min_approved_amount_so_far",
        "distinct_status_in_claim_type",
        "approved_amount_vs_claim_type_avg",
        "last_claimed_amount",
        F.row_number().over(w_all_by_distinct_status_in_cl).alias("seq_no"),
        F.sum("second_approved_amount").over(w_reported_at_quintile_by_seco).alias("running_second_approved_amount"),
        (F.col("last_claimed_amount") - F.lag("last_claimed_amount").over(w_all_by_distinct_status_in_cl)).alias("last_claimed_amount_change"),
        F.sum("approved_amount").over(w_approved_amount_dense_rank_s).alias("approved_amount_dense_rank_status_sum_approved_a"),
    )
)

result = (
    metrics_2
    .filter(F.col("status").isin("OPEN", "APPROVED", "PAID"))
    .select(
        "id",
        "approved_amount",
        "status",
        "approved_amount_share_of_claim_type_adjuster_id",
        "reported_at_quintile",
        "claim_type_adjuster_id_max_approved_amount",
        "min_approved_amount_so_far",
        "distinct_status_in_claim_type",
        "approved_amount_vs_claim_type_avg",
        "running_second_approved_amount",
        F.when(F.col("last_claimed_amount") >= 29336.0, "large").when(F.col("last_claimed_amount") >= 13635.1, "medium").otherwise("small").alias("last_claimed_amount_tier"),
        F.round(F.col("seq_no") / F.nullif(F.col("reported_at_quintile"), F.lit(0)), 3).alias("seq_no_per_reported_at_quintile"),
    )
    .orderBy(F.col("running_second_approved_amount").desc())
)

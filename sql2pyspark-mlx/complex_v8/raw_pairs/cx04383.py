from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id")
w_status_prev2_reported_at_mon = Window.partitionBy("status", F.month("prev2_reported_at")).orderBy("prev2_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_prev2_reported_at_mon_2 = Window.partitionBy("status", F.month("prev2_reported_at")).orderBy("prev2_reported_at", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_distinct_policy_id_in = Window.orderBy(F.col("distinct_policy_id_in_status_claim_type").desc_nulls_first())
w_status = Window.partitionBy("status")
w_all_by_rows_in_status_claim = Window.orderBy("rows_in_status_claim_type", "id")
w_all_by_rows_in_status_claim_2 = Window.orderBy(F.col("rows_in_status_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) | (F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.count(F.lit(1)).over(w_status_claim_type).alias("rows_in_status_claim_type"),
        F.last("claimed_amount").over(w_policy_id_by_approved_amount).alias("last_claimed_amount"),
        F.dense_rank().over(w_all_by_approved_amount).alias("approved_amount_dense_rank"),
        F.lag("reported_at", 2).over(w_policy_id_by_approved_amount_2).alias("prev2_reported_at"),
        F.round(F.avg("claimed_amount").over(w_status_claim_type), 2).alias("status_claim_type_avg_claimed_amount"),
        F.lead("reported_at").over(w_policy_id_by_claimed_amount).alias("next_reported_at"),
        F.min("claimed_amount").over(w_status_claim_type).alias("status_claim_type_min_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_by_approved_amount_2)).alias("claimed_amount_change"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_policy_id_by_approved_amount_2))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.size(F.collect_set("policy_id").over(w_status_claim_type)).alias("distinct_policy_id_in_status_claim_type"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_claim_type), 2).alias("approved_amount_vs_status_claim_type_avg"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.last("reported_at").over(w_status_prev2_reported_at_mon).alias("last_reported_at"),
        F.row_number().over(w_status_prev2_reported_at_mon_2).alias("prev2_reported_at_seq"),
        F.sum("claimed_amount_change").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount_change"),
        F.dense_rank().over(w_all_by_distinct_policy_id_in).alias("distinct_policy_id_in_status_claim_type_dense_ra"),
        F.size(F.collect_set("claim_type").over(w_status)).alias("distinct_claim_type_in_status"),
        F.round(F.col("rows_in_status_claim_type") - F.avg("rows_in_status_claim_type").over(w_status), 2).alias("rows_in_status_claim_type_vs_status_avg"),
        F.lead("claimed_amount").over(w_all_by_rows_in_status_claim).alias("next_claimed_amount"),
        F.first("next_reported_at", ignorenulls=True).over(w_all_by_rows_in_status_claim_2).alias("first_known_next_reported_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "last_claimed_amount",
        "prev2_reported_at",
        "next_reported_at",
        "status_claim_type_min_claimed_amount",
        "distinct_policy_id_in_status_claim_type",
        "approved_amount_vs_status_claim_type_avg",
        "last_reported_at",
        "prev2_reported_at_seq",
        "adjuster_id_sum_claimed_amount_change",
        "rows_in_status_claim_type_vs_status_avg",
        "next_claimed_amount",
        "first_known_next_reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.abs(F.col("approved_amount") - 14068.2).alias("approved_amount_distance"),
    )
    .orderBy(F.col("first_known_next_reported_at").desc())
)

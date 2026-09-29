from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_policy_id = Window.partitionBy("status", "policy_id")
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy("reported_at", "id")
w_policy_id_adjuster_id_by_rep = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) | (F.col("status").isin("REJECTED", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

windowed = (
    valid_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "status_len",
        F.count(F.lit(1)).over(w_status_policy_id).alias("rows_in_status_policy_id"),
        F.lead("claim_date", 2).over(w_adjuster_id_by_reported_at).alias("next2_claim_date"),
        F.first("reported_at").over(w_policy_id_adjuster_id_by_rep).alias("first_reported_at"),
        F.round(F.percent_rank().over(w_all_by_reported_at_ties), 3).alias("reported_at_percent_rank"),
        F.row_number().over(w_adjuster_id_by_reported_at).alias("seq_no"),
        F.size(F.collect_set("adjuster_id").over(w_status_policy_id)).alias("distinct_adjuster_id_in_status_policy_id"),
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_status_policy_id), F.lit(0)), 4).alias("status_len_share_of_status_policy_id"),
        F.first("claim_date", ignorenulls=True).over(w_all_by_reported_at_running).alias("first_known_claim_date"),
        F.max("status_len").over(w_status_policy_id).alias("status_policy_id_max_status_len"),
    )
)

annotated = (
    windowed
    .filter(F.col("claim_date") < F.lit("2025-04-25").cast("date"))
    .select(
        "*",
        F.abs(F.col("approved_amount") - 14377.6).alias("approved_amount_distance"),
    )
)

result = (
    annotated
    .filter(F.col("reported_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        "status_len",
        "rows_in_status_policy_id",
        "first_reported_at",
        "reported_at_percent_rank",
        "distinct_adjuster_id_in_status_policy_id",
        "status_len_share_of_status_policy_id",
        "first_known_claim_date",
        "status_policy_id_max_status_len",
        "approved_amount_distance",
    )
    .orderBy(F.col("adjuster_id").desc_nulls_first(), F.col("id").desc())
    .limit(20)
)

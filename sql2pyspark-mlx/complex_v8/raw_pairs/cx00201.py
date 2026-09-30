from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group_by_days_since_r = Window.partitionBy("status_group").orderBy("days_since_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_days_since_reporte = Window.partitionBy("status").orderBy("days_since_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_status_group_by_days_since_r_2 = Window.partitionBy("status_group").orderBy("days_since_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_status_by_report = Window.partitionBy("adjuster_id", "status").orderBy("reported_at", "id")
w_status_group_by_days_since_r_3 = Window.partitionBy("status_group").orderBy("days_since_reported_at")
w_status_by_days_since_reporte_2 = Window.partitionBy("status").orderBy("days_since_reported_at")
w_claim_type = Window.partitionBy("claim_type")
w_days_since_reported_at_dense = Window.partitionBy("days_since_reported_at_dense_rank", "claim_type").orderBy("running_days_since_reported_at", "id")

filtered = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(10, 16))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("status").isin("APPROVED", "REJECTED"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "status_group",
        "days_since_reported_at",
        F.first("days_since_reported_at", ignorenulls=True).over(w_status_group_by_days_since_r).alias("first_known_days_since_reported_at"),
        F.sum("days_since_reported_at").over(w_status_by_days_since_reporte).alias("running_days_since_reported_at"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.nth_value("claimed_amount", 2).over(w_status_group_by_days_since_r_2).alias("second_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_adjuster_id)).alias("distinct_claim_type_in_adjuster_id"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_claimed_amount"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.ntile(5).over(w_adjuster_id_status_by_report).alias("reported_at_quintile"),
        F.dense_rank().over(w_status_group_by_days_since_r_3).alias("days_since_reported_at_dense_rank"),
        F.dense_rank().over(w_adjuster_id_status_by_report).alias("reported_at_dense_rank"),
        F.dense_rank().over(w_status_by_days_since_reporte_2).alias("days_since_reported_at_dense_rank_2"),
    )
)

derived = (
    metrics
    .filter(F.col("running_days_since_reported_at") < 5250)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "days_since_reported_at",
        "running_days_since_reported_at",
        "second_claimed_amount",
        "adjuster_id_avg_claimed_amount",
        "days_since_reported_at_dense_rank",
        "reported_at_dense_rank",
        "days_since_reported_at_dense_rank_2",
        F.round(F.col("second_claimed_amount") * 1.1, 2).alias("second_claimed_amount_adj"),
        F.concat_ws("|", F.col("status_group"), F.col("claim_type")).alias("status_group_claim_type_label"),
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.round(F.col("days_since_reported_at") / F.nullif(F.sum("days_since_reported_at").over(w_claim_type), F.lit(0)), 4).alias("days_since_reported_at_share_of_claim_type"),
        F.round(F.avg("days_since_reported_at").over(w_claim_type), 2).alias("claim_type_avg_days_since_reported_at"),
        F.lead("second_claimed_amount_adj").over(w_days_since_reported_at_dense).alias("next_second_claimed_amount_adj"),
        F.dense_rank().over(w_days_since_reported_at_dense).alias("running_days_since_reported_at_dense_rank"),
        F.row_number().over(w_days_since_reported_at_dense).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "days_since_reported_at",
        "running_days_since_reported_at",
        "adjuster_id_avg_claimed_amount",
        "days_since_reported_at_dense_rank",
        "reported_at_dense_rank",
        "days_since_reported_at_dense_rank_2",
        "second_claimed_amount_adj",
        "status_group_claim_type_label",
        "claimed_amount_adj",
        "days_since_reported_at_share_of_claim_type",
        "claim_type_avg_days_since_reported_at",
        "next_second_claimed_amount_adj",
        "running_days_since_reported_at_dense_rank",
        "seq_no",
        F.abs(F.col("second_claimed_amount") - 20461.6).alias("second_claimed_amount_distance"),
        F.when(F.col("seq_no") >= 6, "large").when(F.col("seq_no") >= 5, "medium").otherwise("small").alias("seq_no_tier"),
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last(), "id")
    .limit(5)
)

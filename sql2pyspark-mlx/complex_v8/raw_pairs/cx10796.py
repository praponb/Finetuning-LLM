from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_moving_1 = Window.orderBy("reported_at", "id").rowsBetween(-1, Window.currentRow)
w_reported_at_month_adjuster_i = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_id")
w_policy_id_adjuster_level_by = Window.partitionBy("policy_id", "adjuster_level").orderBy(F.col("adjuster_level").desc(), "id")
w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_id").orderBy("approved_amount", "id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_label",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.round(F.avg("adjuster_level").over(w_all_by_reported_at_moving_1), 2).alias("adjuster_level_avg_2"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_reported_at_month_adjuster_i), 2).alias("adjuster_level_vs_reported_at_month_adjuster_id_"),
        F.dense_rank().over(w_policy_id_adjuster_level_by).alias("adjuster_level_dense_rank"),
        F.round(F.cume_dist().over(w_reported_at_to_date_adjuster), 3).alias("approved_amount_cume_dist"),
        F.count(F.lit(1)).over(w_all_by_reported_at_running).alias("running_count"),
        F.sum("claimed_amount").over(w_reported_at_month_adjuster_i).alias("reported_at_month_adjuster_id_sum_claimed_amount"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_label",
        "adjuster_level",
        "adjuster_name",
        "adjuster_level_vs_reported_at_month_adjuster_id_",
        "adjuster_level_dense_rank",
        "approved_amount_cume_dist",
        "running_count",
        "reported_at_month_adjuster_id_sum_claimed_amount",
        F.concat_ws("-", F.col("claim_type"), F.col("claim_date_label")).alias("claim_type_claim_date_label_label"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claim_date = Window.partitionBy("claim_type").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_policy_id_adjuster_id_by_day = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("days_since_reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy(F.col("claim_date").asc_nulls_last())
w_policy_id_adjuster_id_by_day_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("days_since_reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_claims
    .filter((F.col("status") == "PAID") & (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_by_claim_date)).alias("days_since_prev_claim_date"),
        F.lead("claim_date", 3).over(w_claim_type_by_claim_date).alias("next3_claim_date"),
        F.sum("days_since_reported_at").over(w_status_adjuster_id).alias("status_adjuster_id_sum_days_since_reported_at"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.last("approved_amount").over(w_policy_id_adjuster_id_by_day).alias("last_approved_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_claim_date_tie), 3).alias("claim_date_percent_rank"),
        F.sum("approved_amount").over(w_policy_id_adjuster_id_by_day_2).alias("approved_amount_sum_3"),
    )
)

summary = (
    with_windows
    .filter(F.col("status_adjuster_id_sum_days_since_reported_at") < 5600)
    .groupBy("claim_type")
    .agg(
        F.count(F.when(F.col("status").isin("IN_REVIEW", "OPEN"), 1)).alias("n_status_in_in_review_open"),
        F.sum(F.when(F.col("status").isin("REJECTED", "APPROVED", "OPEN"), 1).otherwise(0)).alias("n_status_in_rejected_approved"),
        F.count(F.when(~F.col("claim_type").isin("delay", "fire"), 1)).alias("n_claim_type_not_in_delay_fire"),
        F.count("*").alias("cnt"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("rows_in_status_adjuster_id"), F.lit(0)), 3).alias("approved_amount_to_rows_in_status_adjuster_id_ra"),
    )
)

result = (
    summary
    .filter(F.col("n_claim_type_not_in_delay_fire").between(1, 10))
    .select(
        "claim_type",
        "n_status_in_in_review_open",
        "n_status_in_rejected_approved",
        "n_claim_type_not_in_delay_fire",
        "cnt",
        "approved_amount_to_rows_in_status_adjuster_id_ra",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(
        F.col("approved_amount_to_rows_in_status_adjuster_id_ra").desc_nulls_first(),
        F.col("cnt").desc(),
    )
)

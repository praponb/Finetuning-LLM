from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount")
w_all_by_adjuster_level_full = Window.orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_adjuster_level = Window.orderBy("adjuster_level", "id")
w_status = Window.partitionBy("status")
w_all_by_adjuster_level_prior = Window.orderBy("adjuster_level", "id").rowsBetween(-2, -1)

cleaned = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2024-12-22").cast("date"))
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
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.col("a.team").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_date",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("p.product").alias("policy_product"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "approved_amount",
        "status",
        "adjuster_level",
        "adjuster_name",
        F.dense_rank().over(w_policy_id_claim_type_by_appr).alias("approved_amount_dense_rank"),
        F.nth_value("claim_date", 2).over(w_all_by_adjuster_level_full).alias("second_claim_date"),
        F.sum("adjuster_level").over(w_adjuster_id).alias("adjuster_id_sum_adjuster_level"),
        F.ntile(3).over(w_all_by_reported_at).alias("reported_at_tercile"),
        F.min("approved_amount").over(w_adjuster_id).alias("adjuster_id_min_approved_amount"),
        F.row_number().over(w_all_by_adjuster_level).alias("adjuster_level_seq"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.sum("approved_amount").over(w_status).alias("status_sum_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id"),
        F.max("adjuster_level").over(w_all_by_adjuster_level_prior).alias("adjuster_level_max_2_prior"),
    )
)

scored = (
    windowed
    .filter(F.col("adjuster_id_sum_adjuster_level") >= 23)
    .select(
        "*",
        (F.floor(F.col("reported_at_tercile") / 2) * 2).alias("reported_at_tercile_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "approved_amount",
        "approved_amount_dense_rank",
        "second_claim_date",
        "adjuster_id_sum_adjuster_level",
        "reported_at_tercile",
        "adjuster_id_min_approved_amount",
        "rows_in_status",
        "status_sum_approved_amount",
        "approved_amount_share_of_adjuster_id",
        "reported_at_tercile_bucket",
        F.when(F.col("status").isin("PAID", "APPROVED"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        (F.col("rows_in_status") + F.col("adjuster_level_seq")).alias("rows_in_status_plus_adjuster_level_seq"),
    )
    .orderBy(F.col("approved_amount").desc())
)

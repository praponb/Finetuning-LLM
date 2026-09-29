from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status_or_unknow = Window.partitionBy("adjuster_id", "status_or_unknown")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_status_or_unknown_holder_seg = Window.partitionBy("status_or_unknown", "holder_segment")
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_or_unknown_claim_type = Window.partitionBy("status_or_unknown", "claim_type")
w_status_or_unknown_by_approve = Window.partitionBy("status_or_unknown").orderBy("approved_amount")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id")

base = (
    df_claims
    .filter(F.col("approved_amount").between(0.0, 23487.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_details = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.segment").alias("holder_segment"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.size(F.collect_set("holder_segment").over(w_adjuster_id_status_or_unknow)).alias("distinct_holder_segment_in_adjuster_id_status_or"),
        F.ntile(5).over(w_all_by_reported_at).alias("reported_at_quintile"),
        F.count(F.lit(1)).over(w_adjuster_id_status_or_unknow).alias("rows_in_adjuster_id_status_or_unknown"),
        F.min("approved_amount").over(w_status_or_unknown_holder_seg).alias("status_or_unknown_holder_segment_min_approved_am"),
        F.count(F.lit(1)).over(w_status_or_unknown_holder_seg).alias("rows_in_status_or_unknown_holder_segment"),
        F.nth_value("policy_end_date", 2).over(w_all_by_reported_at_full).alias("second_policy_end_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_status_or_unknow), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_status_or_un"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_or_unknown_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_status_or_unknown_claim_"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_status_or_unknow)).alias("distinct_policy_id_in_adjuster_id_status_or_unkn"),
        F.last("claimed_amount").over(w_all_by_reported_at_full).alias("last_claimed_amount"),
        F.count(F.lit(1)).over(w_status_or_unknown_claim_type).alias("rows_in_status_or_unknown_claim_type"),
        F.dense_rank().over(w_status_or_unknown_by_approve).alias("approved_amount_dense_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_type_by_reported_at))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("approved_amount_dense_rank") <= 2) & (F.year(F.col("claim_date")) == 2025))
    .drop("approved_amount_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        "status_or_unknown",
        "policy_end_date",
        "distinct_holder_segment_in_adjuster_id_status_or",
        "reported_at_quintile",
        "rows_in_adjuster_id_status_or_unknown",
        "status_or_unknown_holder_segment_min_approved_am",
        "rows_in_status_or_unknown_holder_segment",
        "second_policy_end_date",
        "claimed_amount_share_of_adjuster_id_status_or_un",
        "distinct_policy_id_in_adjuster_id_status_or_unkn",
        "last_claimed_amount",
        "rows_in_status_or_unknown_claim_type",
        "minutes_since_prev_reported_at",
    )
    .orderBy(
        "approved_amount",
        F.col("status_or_unknown_holder_segment_min_approved_am").desc_nulls_first(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment_by_greatest_a = Window.partitionBy("holder_segment").orderBy("greatest_approved_amount_claimed_amount", "id")
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_greatest_approved_amo = Window.orderBy(F.col("greatest_approved_amount_claimed_amount").asc_nulls_last(), "id")
w_policy_product_by_holder_ris = Window.partitionBy("policy_product").orderBy(F.col("holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_greatest_approved_amo_2 = Window.orderBy(F.col("greatest_approved_amount_claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

combined_details_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.greatest_approved_amount_claimed_amount",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    combined_details_policy
    .select(
        "*",
        F.row_number().over(w_holder_segment_by_greatest_a).alias("rn"),
        F.min("holder_risk_score").over(w_policy_id_adjuster_id).alias("policy_id_adjuster_id_min_holder_risk_score"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.lead("approved_amount", 1, 0).over(w_all_by_greatest_approved_amo).alias("next_approved_amount"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_policy_id_adjuster_id), 2).alias("holder_risk_score_vs_policy_id_adjuster_id_avg"),
        F.row_number().over(w_all_by_greatest_approved_amo).alias("rn_2"),
        F.first("claim_date", ignorenulls=True).over(w_policy_product_by_holder_ris).alias("first_known_claim_date"),
        F.last("holder_risk_score").over(w_all_by_greatest_approved_amo_2).alias("last_holder_risk_score"),
    )
)

derived = (
    windowed
    .filter(F.col("next_approved_amount") > 2349.3)
    .select(
        "*",
        F.least(F.col("next_approved_amount"), F.col("rn_2")).alias("least_next_approved_amount_rn_2"),
        F.concat_ws("-", F.col("policy_product"), F.col("status")).alias("policy_product_status_label"),
    )
)

result = (
    derived
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "greatest_approved_amount_claimed_amount",
        "policy_end_date",
        "holder_segment",
        "holder_risk_score",
        "rn",
        "policy_id_adjuster_id_min_holder_risk_score",
        "next_approved_amount",
        "rn_2",
        "first_known_claim_date",
        "last_holder_risk_score",
        "least_next_approved_amount_rn_2",
        "policy_product_status_label",
        F.coalesce(F.col("policy_status"), F.lit("UNKNOWN")).alias("policy_status_or_unknown"),
    )
)

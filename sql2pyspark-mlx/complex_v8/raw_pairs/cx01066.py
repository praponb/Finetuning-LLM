from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")

base = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_details_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

sequenced = (
    with_details_policy
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status), F.lit(0)), 4).alias("approved_amount_share_of_status"),
        F.sum("approved_amount").over(w_status).alias("status_sum_approved_amount"),
        F.size(F.collect_set("policy_holder_id").over(w_status)).alias("distinct_policy_holder_id_in_status"),
        F.size(F.collect_set("claim_type").over(w_status)).alias("distinct_claim_type_in_status"),
    )
)

with_parent = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "inner")
    .filter(F.col("p.segment") != "VIP")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.policy_holder_id",
        "s.approved_amount_share_of_status",
        "s.status_sum_approved_amount",
        "s.distinct_policy_holder_id_in_status",
        "s.distinct_claim_type_in_status",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "policy_holder_id",
        "approved_amount_share_of_status",
        "holder_segment",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
        F.when(F.col("claim_type").isin("theft", "delay"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
    )
    .orderBy(F.col("claim_type").asc_nulls_last(), "claim_date")
)

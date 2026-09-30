from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type_by_approve = Window.partitionBy("status", "claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").asc_nulls_last())
w_policy_status_claim_type_by = Window.partitionBy("policy_status", "claim_type").orderBy(F.col("approved_amount").desc())
w_policy_holder_id = Window.partitionBy("policy_holder_id")
w_policy_status = Window.partitionBy("policy_status")
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_approved = Window.partitionBy("policy_holder_id").orderBy("approved_amount")

recent_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

with_parent_policy = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.status_slug",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    with_parent_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_slug",
        "policy_holder_id",
        "policy_status",
        F.nth_value("approved_amount", 2).over(w_status_claim_type_by_approve).alias("second_approved_amount"),
        F.rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_rank"),
        F.round(F.percent_rank().over(w_policy_status_claim_type_by), 3).alias("approved_amount_percent_rank"),
        F.round(F.avg("approved_amount").over(w_policy_holder_id), 2).alias("policy_holder_id_avg_approved_amount"),
        F.round(F.avg("claimed_amount").over(w_policy_status), 2).alias("policy_status_avg_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_status).alias("rows_in_policy_status"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist"),
        F.first("approved_amount").over(w_all_by_claimed_amount_full).alias("first_approved_amount"),
        F.round(F.percent_rank().over(w_policy_holder_id_by_approved), 3).alias("approved_amount_percent_rank_2"),
    )
)

top_rows = sequenced.filter((F.col("claimed_amount_rank") == 1) & (F.col("adjuster_id").isNotNull()))

result = (
    top_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_slug",
        "policy_holder_id",
        "policy_status",
        "second_approved_amount",
        "approved_amount_percent_rank",
        "policy_holder_id_avg_approved_amount",
        "policy_status_avg_claimed_amount",
        "rows_in_policy_status",
        "claimed_amount_cume_dist",
        "approved_amount_percent_rank_2",
        F.coalesce(F.col("rows_in_policy_status"), F.lit(0)).alias("rows_in_policy_status_filled"),
        F.round(F.col("second_approved_amount") * 0.9, 2).alias("second_approved_amount_adj"),
    )
    .orderBy(F.col("approved_amount").desc_nulls_first())
)

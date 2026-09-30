from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_or_unknown_adjust = Window.partitionBy("claim_type_or_unknown", "adjuster_id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_status_claim_type_or_unknown = Window.partitionBy("status", "claim_type_or_unknown").orderBy(F.col("claim_type_or_unknown_adjuster_id_max_claimed_am").desc(), "id")

recent_claims = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

ranked = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "status",
        "claim_type_or_unknown",
        F.round(F.avg("approved_amount").over(w_claim_type_or_unknown_adjust), 2).alias("claim_type_or_unknown_adjuster_id_avg_approved_a"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.max("claimed_amount").over(w_claim_type_or_unknown_adjust).alias("claim_type_or_unknown_adjuster_id_max_claimed_am"),
    )
)

enriched_policy = (
    ranked.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.status",
        "s.claim_type_or_unknown",
        "s.claim_type_or_unknown_adjuster_id_max_claimed_am",
        F.col("p.status").alias("policy_status"),
    )
)

sequenced = (
    enriched_policy
    .select(
        "id",
        "policy_id",
        "claim_type_or_unknown_adjuster_id_max_claimed_am",
        "policy_status",
        F.ntile(10).over(w_status_claim_type_or_unknown).alias("claim_type_or_unknown_adjuster_id_max_claimed_am_2"),
        F.round(F.percent_rank().over(w_status_claim_type_or_unknown), 3).alias("claim_type_or_unknown_adjuster_id_max_claimed_am_3"),
        F.rank().over(w_status_claim_type_or_unknown).alias("claim_type_or_unknown_adjuster_id_max_claimed_am_4"),
    )
)

result = (
    sequenced
    .groupBy("claim_type_or_unknown_adjuster_id_max_claimed_am_2", "claim_type_or_unknown_adjuster_id_max_claimed_am_4")
    .pivot("policy_status", ["CANCELLED", "ACTIVE"])
    .agg(F.sum("claim_type_or_unknown_adjuster_id_max_claimed_am"))
)

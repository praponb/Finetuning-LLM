from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy("claimed_amount", "id")
w_status_policy_id_by_approved = Window.partitionBy("status", "policy_id").orderBy(F.col("approved_amount_distance").desc(), "id")

filtered = (
    df_claims
    .filter((F.col("claimed_amount").between(10673.3, 46461.4)) & (F.col("approved_amount").between(0.0, 32510.7)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.abs(F.col("approved_amount") - 2887.3).alias("approved_amount_distance"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "approved_amount_bucket",
        "claim_type_clean",
        "approved_amount_distance",
        F.row_number().over(w_status_adjuster_id_by_claime).alias("claimed_amount_seq"),
        F.lead("claim_date").over(w_status_policy_id_by_approved).alias("next_claim_date"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.status").alias("policy_status"))
)

result = (
    enriched
    .groupingSets(
        [["status", "policy_id"], ["status"]],
        "status", "policy_id",
    )
    .agg(
        F.sum(F.when(~F.col("status").isin("IN_REVIEW", "APPROVED"), 1).otherwise(0)).alias("n_status_not_in_in_review_appr"),
        F.min("approved_amount_bucket").alias("min_approved_amount_bucket"),
        F.min("claim_date").alias("first_claim_date"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "policy_id",
        "n_status_not_in_in_review_appr",
        "min_approved_amount_bucket",
        "first_claim_date",
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)

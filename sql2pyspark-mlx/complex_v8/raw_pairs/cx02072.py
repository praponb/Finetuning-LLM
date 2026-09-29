from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_all_by_reported_at_date_ties = Window.orderBy(F.col("reported_at_date").desc())
w_all_by_reported_at_week_full = Window.orderBy(F.col("reported_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_adjuster_id_claim_typ = Window.orderBy("adjuster_id_claim_type_min_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_status = Window.partitionBy("policy_id", "status")

base_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 13)) & (F.col("adjuster_id") <= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.date_trunc("week", F.col("reported_at")).alias("reported_at_week"),
    )
)

windowed = (
    base_claims
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_claim_type), 2).alias("claimed_amount_vs_adjuster_id_claim_type_avg"),
        F.min("approved_amount").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_min_approved_amount"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_claim_type), 2).alias("adjuster_id_claim_type_avg_claimed_amount"),
        F.round(F.cume_dist().over(w_all_by_reported_at_date_ties), 3).alias("reported_at_date_cume_dist"),
        F.last("reported_at_date").over(w_all_by_reported_at_week_full).alias("last_reported_at_date"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_claim_type"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claim_date").isNotNull()), "left_semi")
)

metrics = (
    with_activity
    .select(
        "*",
        F.nth_value("reported_at_date", 2).over(w_all_by_adjuster_id_claim_typ).alias("second_reported_at_date"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_status)).alias("distinct_adjuster_id_in_policy_id_status"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_status"),
    )
)

result = (
    metrics
    .cube("adjuster_id", "policy_id")
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.sum(F.when(F.col("status").isin("PAID", "IN_REVIEW"), F.col("reported_at_date_cume_dist"))).alias("reported_at_date_cume_dist_status_in_paid_in_rev"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

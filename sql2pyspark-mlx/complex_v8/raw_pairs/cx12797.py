from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_claim_date_month_by_p = Window.partitionBy("status", F.date_trunc("month", "claim_date")).orderBy("policy_end_date", "id")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_claim_type_policy_id_by_clai_2 = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_policy_end_date_month = Window.partitionBy(F.month("policy_end_date"))

prepared_claims = df_claims.filter(F.col("claimed_amount") > 18778.6)

enriched = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.year(F.col("p.end_date")) == 2026)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "policy_end_date",
        F.max("claimed_amount").over(w_status).alias("status_max_claimed_amount"),
        F.sum("approved_amount").over(w_status).alias("status_sum_approved_amount"),
        F.round(F.avg("approved_amount").over(w_status), 2).alias("status_avg_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status), F.lit(0)), 4).alias("approved_amount_share_of_status"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_status_claim_date_month_by_p)).alias("days_since_prev_claim_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_policy_id_by_clai)).alias("days_since_prev_claim_date_2"),
        F.min("approved_amount").over(w_status).alias("status_min_approved_amount"),
        F.first("claimed_amount").over(w_claim_type_policy_id_by_clai_2).alias("first_claimed_amount"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "policy_end_date",
        "status_max_claimed_amount",
        "status_sum_approved_amount",
        "status_avg_approved_amount",
        "days_since_prev_claim_date_2",
        "status_min_approved_amount",
        F.lead("approved_amount").over(w_all_by_claim_date).alias("next_approved_amount"),
        F.round(F.col("days_since_prev_claim_date") - F.avg("days_since_prev_claim_date").over(w_adjuster_id), 2).alias("days_since_prev_claim_date_vs_adjuster_id_avg"),
        F.row_number().over(w_all_by_reported_at).alias("reported_at_seq"),
        F.round(F.cume_dist().over(w_all_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
        F.max("status_max_claimed_amount").over(w_policy_end_date_month).alias("policy_end_date_month_max_status_max_claimed_amo"),
    )
)

result = (
    with_trends
    .filter(F.col("days_since_prev_claim_date_vs_adjuster_id_avg") >= -40.0)
    .select(
        "days_since_prev_claim_date_vs_adjuster_id_avg",
        "approved_amount",
        "status_avg_approved_amount",
    )
    .distinct()
    .orderBy(F.col("approved_amount").asc())
)

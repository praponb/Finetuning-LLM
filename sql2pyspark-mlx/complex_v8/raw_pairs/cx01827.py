from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_reported_at_month_by = Window.partitionBy("status", F.month("reported_at")).orderBy("reported_at")
w_status_policy_id = Window.partitionBy("status", "policy_id")

valid = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "approved_amount_filled",
        "claim_type_status_label",
        F.rank().over(w_status_reported_at_month_by).alias("reported_at_rank"),
        F.count(F.lit(1)).over(w_status_policy_id).alias("rows_in_status_policy_id"),
        F.sum("claimed_amount").over(w_status_policy_id).alias("status_policy_id_sum_claimed_amount"),
    )
)

combined_details_policy = (
    ranked.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product") != "AUTO"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .filter(F.col("p.end_date").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.approved_amount_filled",
        "s.reported_at_rank",
        "s.rows_in_status_policy_id",
        "s.status_policy_id_sum_claimed_amount",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

totals = (
    combined_details_policy
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "adjuster_id",
        "adjuster_level",
    )
    .agg(
        F.sum("adjuster_level").alias("total_adjuster_level"),
        F.array_sort(F.collect_set("adjuster_team")).alias("adjuster_team_list"),
    )
)

result = (
    totals
    .select(
        "claim_date_month",
        "adjuster_level",
        "total_adjuster_level",
        "adjuster_team_list",
    )
    .orderBy(
        F.col("claim_date_month").desc_nulls_first(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_by_reporte = Window.partitionBy("status_or_unknown").orderBy("reported_at")
w_status_or_unknown_by_reporte_2 = Window.partitionBy("status_or_unknown").orderBy("reported_at", "id").rowsBetween(-2, Window.currentRow)
w_status_status_or_unknown = Window.partitionBy("status", "status_or_unknown")
w_status_or_unknown_by_reporte_3 = Window.partitionBy("status_or_unknown").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_or_unknown_by_reporte_4 = Window.partitionBy("status_or_unknown").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_claims
    .filter(F.col("approved_amount") > 7204.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

with_details_adjuster = (
    base.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.days_since_reported_at",
        "s.status_or_unknown",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

ranked = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "status",
        "days_since_reported_at",
        "status_or_unknown",
        "adjuster_team",
        "policy_start_date",
        F.dense_rank().over(w_status_or_unknown_by_reporte).alias("reported_at_dense_rank"),
        F.sum("claimed_amount").over(w_status_or_unknown_by_reporte_2).alias("claimed_amount_sum_3"),
        F.round(F.avg("days_since_reported_at").over(w_status_status_or_unknown), 2).alias("status_status_or_unknown_avg_days_since_reported"),
        F.round(F.cume_dist().over(w_status_or_unknown_by_reporte), 3).alias("reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_status_or_unknown_by_reporte_3).alias("running_count"),
        F.first("reported_at").over(w_status_or_unknown_by_reporte_4).alias("first_reported_at"),
    )
)

with_children = (
    ranked.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.status",
        "s.days_since_reported_at",
        "s.adjuster_team",
        "s.policy_start_date",
        "s.reported_at_dense_rank",
        "s.status_status_or_unknown_avg_days_since_reported",
        "s.running_count",
        "s.first_reported_at",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    with_children
    .filter(F.col("claim_date").between(F.lit("2024-02-23").cast("date"), F.lit("2025-09-14").cast("date")))
    .select(
        "id",
        "adjuster_team",
        "policy_start_date",
        "status_status_or_unknown_avg_days_since_reported",
        "running_count",
        "first_reported_at",
        "claim_type",
        "claim_date",
        "claim_approved_amount",
        "claim_reported_at",
    )
    .orderBy("claim_reported_at", "id")
)

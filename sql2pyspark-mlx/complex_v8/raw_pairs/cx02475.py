from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy(F.col("claim_date").desc(), "id")

recent_claims = (
    df_claims
    .filter(F.col("status") != "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.abs(F.col("approved_amount") - 15477.7).alias("approved_amount_distance"),
    )
)

metrics = (
    recent_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "claim_date_quarter",
        "claim_type_status_label",
        "approved_amount_distance",
        F.lead("claim_date_quarter", 2).over(w_adjuster_id_by_claim_date).alias("next2_claim_date_quarter"),
        F.lag("approved_amount_distance").over(w_adjuster_id_by_claim_date).alias("prev_approved_amount_distance"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_quarter",
        "s.claim_type_status_label",
        "s.approved_amount_distance",
        "s.next2_claim_date_quarter",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

with_activity = (
    joined.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(1, 3)) & (F.col("a.team").isNotNull()), "left_anti")
)

line_level = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.claim_date_quarter",
        "s.claim_type_status_label",
        "s.next2_claim_date_quarter",
        "s.adjuster_team",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
    )
)

filtered = (
    line_level.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") < 38909.4), "left_anti")
    .filter(F.col("claim_date_quarter") < F.lit("2025-02-01 09:00:00").cast("timestamp"))
)

result = (
    filtered
    .filter(F.col("adjuster_team").isin("Travel", "Motor", "Property"))
    .select(
        "id",
        "adjuster_id",
        "status",
        "claim_date_quarter",
        "claim_type_status_label",
        "next2_claim_date_quarter",
        "adjuster_team",
        "claim_status",
        "claim_reported_at",
        "claim_type",
    )
    .orderBy(F.col("claim_type").asc_nulls_last())
)

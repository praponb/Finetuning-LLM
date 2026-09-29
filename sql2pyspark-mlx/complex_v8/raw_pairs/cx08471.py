from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("claimed_amount") < 41878.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

with_parent_adjuster = (
    base.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

screened = (
    with_parent_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status") == "OPEN"), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 2), "left_semi")
)

filtered = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("adjuster_level").alias("overall_avg_adjuster_level")))
    .filter(F.col("adjuster_level") > F.col("overall_avg_adjuster_level"))
    .drop("overall_avg_adjuster_level")
)

annotated = filtered.select("*", F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"))

tagged = (
    annotated
    .filter(F.col("claimed_amount") < 33690.0)
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "adjuster_level",
        "reported_at_day",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
    .orderBy(F.col("approved_amount").desc())
)

from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (F.col("approved_amount") < 28426.9))
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
        (F.col("reported_at") + F.expr("INTERVAL 30 MINUTES")).alias("reported_at_plus_30m"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

with_activity = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") > 10518.4) & (F.col("c.claim_date") < F.lit("2025-05-26").cast("date")), "left_anti")
)

enriched_adjuster = (
    with_activity.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

annotated = (
    enriched_adjuster
    .filter(F.col("claimed_amount") < 41878.5)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_prefix",
        "adjuster_level",
        "policy_start_date",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.length(F.trim(F.col("claim_type_prefix"))).alias("claim_type_prefix_len"),
    )
)

derived = (
    annotated
    .select(
        "*",
        F.abs(F.col("adjuster_level") - 3).alias("adjuster_level_distance"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.datediff(F.col("claim_date"), F.col("policy_start_date")).alias("days_policy_start_date_to_claim_date"),
    )
)

result = (
    derived
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "adjuster_level",
        "policy_start_date",
        "approved_amount_plus_claimed_amount",
        "adjuster_level_distance",
        "reported_at_date",
        "days_policy_start_date_to_claim_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at_date")).alias("days_since_reported_at_date"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
    .orderBy("claimed_amount", "id")
    .limit(10)
)

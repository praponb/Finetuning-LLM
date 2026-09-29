from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_details_policy = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-11-25").cast("date")))
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
        F.col("po.segment").alias("holder_segment"),
    )
)

screened = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(1, 2)), "left_semi")
)

enriched = (
    screened.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        "s.policy_end_date",
        "s.policy_status",
        "s.holder_region",
        "s.holder_segment",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    enriched
    .filter(F.col("claim_type") == "fire")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "adjuster_id",
        "product",
        "premium",
        "policy_end_date",
        "holder_region",
        "holder_segment",
        "adjuster_name",
        "policy_premium",
        "policy_start_date",
    )
    .orderBy("policy_end_date", "id")
    .limit(20)
)

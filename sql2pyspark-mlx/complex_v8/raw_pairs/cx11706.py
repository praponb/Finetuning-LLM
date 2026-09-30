from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) & (F.col("adjuster_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

with_activity = (
    scoped_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type") == "theft") & (F.col("c.approved_amount") >= 0.0), "left_anti")
    .filter(F.col("adjuster_id") <= 2)
)

filtered = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") <= 43724.9), "left_anti")
    .filter(F.col("claimed_amount_per_approved_amount") < 7.5)
)

with_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

screened = (
    with_details_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") > 2349.3) & (F.col("c.claim_type") != "theft"), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

summarized_rows = (
    screened.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-02-14").cast("date")), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .filter(F.col("p.start_date").between(F.lit("2024-11-20").cast("date"), F.lit("2025-03-24").cast("date")))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.adjuster_level",
        "s.adjuster_name",
        "s.policy_premium",
        "s.policy_status",
        "s.min_claim_approved_amount",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.risk_score").alias("holder_risk_score"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "adjuster_level",
        "adjuster_name",
        "policy_premium",
        "policy_start_date",
        "holder_risk_score",
        "holder_region",
        F.trunc(F.col("policy_start_date"), "MM").alias("policy_start_date_month_start"),
    )
    .orderBy(F.col("adjuster_name").desc(), F.col("holder_risk_score").desc(), "id")
    .limit(5)
)

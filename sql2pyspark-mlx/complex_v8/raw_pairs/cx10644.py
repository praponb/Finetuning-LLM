from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("approved_amount") > 1364.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.year(F.col("claim_date")).alias("claim_date_year"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled_2"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_year",
        "s.claimed_amount_filled",
        "s.claimed_amount_filled_2",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.team").alias("adjuster_team"),
    )
)

line_level = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.claimed_amount_filled",
        "s.policy_premium",
        "s.policy_start_date",
        "s.adjuster_team",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    line_level
    .cube("adjuster_team", "status", "adjuster_id")
    .agg(
        F.round(F.avg("claim_approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count("*").alias("n_rows"),
        F.countDistinct("adjuster_team").alias("distinct_adjuster_teams"),
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)

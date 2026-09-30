from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("claim_type") == "flood") | (~F.col("status").isin("OPEN", "PAID")))
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
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

enriched_adjuster = (
    scoped_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.least_approved_amount_claimed_amount",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
    )
)

adjuster_id_stats = (
    enriched_adjuster
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("policy_premium", 0.5).alias("median_policy_premium"))
)

vs_group = (
    enriched_adjuster.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.policy_premium") >= F.col("g.median_policy_premium"))
    .select(
        "s.*",
        "g.median_policy_premium",
        F.round(F.col("s.policy_premium") - F.col("g.median_policy_premium"), 2).alias("policy_premium_minus_median"),
    )
)

enriched = (
    vs_group.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.least_approved_amount_claimed_amount",
        "s.adjuster_level",
        "s.adjuster_name",
        "s.adjuster_team",
        "s.policy_premium",
        F.col("p.premium").alias("policy_premium_2"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "least_approved_amount_claimed_amount",
        "adjuster_name",
        "adjuster_team",
        "policy_premium",
        "policy_premium_2",
    )
    .orderBy(F.col("approved_amount").desc(), F.col("status").desc(), "id")
    .limit(20)
)

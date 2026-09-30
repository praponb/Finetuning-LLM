from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 18364.1) & (F.col("adjuster_id").isNotNull()))
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
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

with_parent = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_filled",
        "s.claimed_amount_adj",
        "s.greatest_claimed_amount_approved_amount",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

combined_details = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.holder_id") >= 15)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.claimed_amount_filled",
        "s.claimed_amount_adj",
        "s.greatest_claimed_amount_approved_amount",
        "s.adjuster_team",
        "s.adjuster_name",
        "s.adjuster_level",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

screened = (
    combined_details
    .crossJoin(combined_details.agg(F.min("adjuster_level").alias("overall_min_adjuster_level")))
    .filter(F.col("adjuster_level") > F.col("overall_min_adjuster_level"))
    .drop("overall_min_adjuster_level")
)

adjuster_team_stats = (
    screened
    .groupBy("adjuster_team")
    .agg(F.percentile_approx("claimed_amount_filled", 0.5).alias("median_claimed_amount_filled"))
)

vs_group = (
    screened.alias("s")
    .join(adjuster_team_stats.alias("g"), F.col("g.adjuster_team") == F.col("s.adjuster_team"), "inner")
    .filter(F.col("s.claimed_amount_filled") >= F.col("g.median_claimed_amount_filled"))
    .select(
        "s.*",
        "g.median_claimed_amount_filled",
        F.round(F.col("s.claimed_amount_filled") - F.col("g.median_claimed_amount_filled"), 2).alias("claimed_amount_filled_minus_median"),
    )
)

enriched = (
    vs_group.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount_filled",
        "s.claimed_amount_adj",
        "s.greatest_claimed_amount_approved_amount",
        "s.adjuster_level",
        "s.policy_holder_id",
        "s.claimed_amount_filled_minus_median",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    enriched
    .filter(F.col("adjuster_id") <= 5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount_filled",
        "claimed_amount_adj",
        "greatest_claimed_amount_approved_amount",
        "adjuster_level",
        "policy_holder_id",
        "claimed_amount_filled_minus_median",
        "policy_premium",
        "policy_end_date",
        F.round(F.col("claimed_amount_filled") * 0.9, 2).alias("claimed_amount_filled_adj"),
        F.date_format(F.col("policy_end_date"), "MMM yyyy").alias("policy_end_date_label"),
    )
    .orderBy(
        "claimed_amount_filled",
        F.col("claimed_amount_filled_minus_median").asc_nulls_last(),
    )
)

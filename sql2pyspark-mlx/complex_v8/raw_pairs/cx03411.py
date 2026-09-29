from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("approved_amount") >= 4868.7) & (F.col("status") == "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 32796.4, "HIGH").when(F.col("claimed_amount") >= 19337.6, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

combined_details_adjuster = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.status_len",
        F.col("a.level").alias("adjuster_level"),
    )
)

policy_id_stats = (
    combined_details_adjuster
    .groupBy("policy_id")
    .agg(F.percentile_approx("adjuster_level", 0.5).alias("median_adjuster_level"))
)

outliers = (
    combined_details_adjuster.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.adjuster_level") >= F.col("g.median_adjuster_level"))
    .select(
        "s.*",
        "g.median_adjuster_level",
        F.round(F.col("s.adjuster_level") - F.col("g.median_adjuster_level"), 2).alias("adjuster_level_minus_median"),
    )
)

with_details_adjuster = (
    outliers.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.adjuster_level",
        "s.median_adjuster_level",
        "s.adjuster_level_minus_median",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

enriched_adjuster = (
    with_details_adjuster.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.median_adjuster_level",
        "s.adjuster_level_minus_median",
        "s.adjuster_name",
        "s.adjuster_team",
        "s.policy_start_date",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level_2"),
    )
)

without_matches = (
    enriched_adjuster
    .crossJoin(enriched_adjuster.agg(F.avg("median_adjuster_level").alias("overall_avg_median_adjuster_level")))
    .filter(F.col("median_adjuster_level") > F.col("overall_avg_median_adjuster_level"))
    .drop("overall_avg_median_adjuster_level")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "status",
        "median_adjuster_level",
        "adjuster_level_minus_median",
        "adjuster_team",
        "adjuster_team_2",
        "adjuster_adjuster_name",
    )
    .orderBy(F.col("policy_id").desc_nulls_first(), "adjuster_team_2")
)

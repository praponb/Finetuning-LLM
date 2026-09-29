from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 16404.6).alias("approved_amount_distance"),
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_distance",
        "s.reported_at_month",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

adjuster_id_stats = (
    enriched
    .groupBy("adjuster_id")
    .agg(F.avg("approved_amount_distance").alias("avg_approved_amount_distance"))
)

vs_group = (
    enriched.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount_distance") > F.col("g.avg_approved_amount_distance"))
    .select(
        "s.*",
        "g.avg_approved_amount_distance",
        F.round(F.col("s.approved_amount_distance") - F.col("g.avg_approved_amount_distance"), 2).alias("approved_amount_distance_minus_avg"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") >= 2887.3) & (F.hour(F.col("c.reported_at")).between(7, 10)), "left_semi")
    .filter(F.col("policy_id").isNotNull())
)

result = (
    qualified
    .filter(F.col("approved_amount_distance") > 15000.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_distance",
        "reported_at_month",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level",
        "avg_approved_amount_distance",
        "approved_amount_distance_minus_avg",
        F.concat_ws("|", F.col("adjuster_name"), F.col("adjuster_team")).alias("adjuster_name_adjuster_team_label"),
    )
    .orderBy(F.col("adjuster_name").asc_nulls_last(), F.col("claim_date").desc())
)

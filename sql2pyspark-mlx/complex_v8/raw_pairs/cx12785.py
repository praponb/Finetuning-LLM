from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("policy_id") >= 17) & (F.col("claimed_amount") <= 43724.9))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.abs(F.col("claimed_amount") - 30115.9).alias("claimed_amount_distance"),
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

enriched = (
    valid
    .filter(F.col("claim_type_status_label").isNotNull())
    .select(
        "*",
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Property") & (F.length(F.trim(F.col("a.adjuster_name"))) >= 8), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_reported_at",
    )
)

joined_adjuster = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type_prefix",
        "s.claim_type_status_label",
        "s.reported_at_date",
        "s.last_claim_reported_at",
        "s.claim_count",
        "s.first_claim_reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    joined_adjuster
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at_date",
        "first_claim_reported_at",
        "adjuster_team",
        "adjuster_name",
        F.concat_ws("|", F.col("adjuster_name"), F.col("adjuster_team")).alias("adjuster_name_adjuster_team_label"),
    )
    .orderBy(F.col("policy_id").asc(), F.col("claim_date").asc_nulls_last())
)

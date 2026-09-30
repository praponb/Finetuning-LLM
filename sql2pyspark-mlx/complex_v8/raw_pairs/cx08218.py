from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claimed_amount") > 7204.1))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.claim_date_plus_365d",
        "s.status_or_unknown",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    combined_details
    .groupingSets(
        [["adjuster_id", "policy_id"], ["adjuster_id"], ["policy_id"]],
        "adjuster_id", "policy_id",
    )
    .agg(F.countDistinct("adjuster_team").alias("distinct_adjuster_teams"))
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)

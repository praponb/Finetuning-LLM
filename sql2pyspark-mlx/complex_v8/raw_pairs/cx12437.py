from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter(F.col("team").isin("Health", "Travel", "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.when(F.col("team").isin("Property", "Health"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type") == "theft") & (F.col("c.policy_id") >= 18), "left_anti")
)

detail_rows_claims = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.team",
        "s.level",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "claim_id",
        "claim_type",
        "claim_policy_id",
        "claim_reported_at",
        F.concat(F.col("adjuster_name"), F.lit("#"), F.col("id").cast("string")).alias("adjuster_name_ref"),
    )
)

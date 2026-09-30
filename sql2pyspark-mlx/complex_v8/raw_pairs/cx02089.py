from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("claim_type") == "flood") & (F.col("status") == "PAID"))
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

joined_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_clean",
        "s.claim_date_year",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    joined_adjuster
    .cube("adjuster_team", "adjuster_id")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

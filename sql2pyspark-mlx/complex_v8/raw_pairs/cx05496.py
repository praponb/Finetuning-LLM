from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 11)) & (~F.col("claim_type").isin("theft", "medical")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

filtered = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount").between(3192.1, 32441.2)) & (F.col("c.policy_id") <= 3), "left_semi")
    .filter(F.col("status_prefix").isNotNull())
)

with_details = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status_prefix",
        "s.reported_at_date",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

summary = (
    with_details
    .groupBy(
        F.year("claim_date").alias("claim_date_year"),
        "policy_id",
        "adjuster_team",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("adjuster_level").alias("max_adjuster_level"),
        F.sum("adjuster_level").alias("total_adjuster_level"),
        F.countDistinct("adjuster_team").alias("distinct_adjuster_teams"),
    )
    .filter(F.col("total_adjuster_level") > 2)
)

result = (
    summary
    .select(
        "claim_date_year",
        "policy_id",
        F.col("row_count").alias("m_row_count"),
        F.col("total_adjuster_level").alias("total_adjuster_level_v"),
        F.col("distinct_adjuster_teams").alias("distinct_adjuster_teams_v"),
        F.col("max_adjuster_level").alias("m_max_adjuster_level"),
    )
    .unpivot(
        ids=["claim_date_year", "policy_id"],
        values=["m_row_count", "total_adjuster_level_v", "distinct_adjuster_teams_v", "m_max_adjuster_level"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

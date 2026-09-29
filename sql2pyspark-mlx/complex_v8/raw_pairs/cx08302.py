from pyspark.sql import functions as F

prepared_adjusters = (
    df_adjusters
    .filter(~F.col("team").isin("Travel", "Property"))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.initcap(F.trim(F.col("team"))).alias("team_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

profiled = (
    prepared_adjusters.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_children = (
    profiled.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.team",
        "s.level",
        "s.team_clean",
        "s.claim_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    with_children
    .select(
        "id",
        "level",
        "claim_count",
        "claim_id",
        "claim_reported_at",
        F.date_trunc("week", F.col("claim_reported_at")).alias("claim_reported_at_week"),
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
    )
    .orderBy(F.col("claim_reported_at_week").asc_nulls_last(), "claim_id", "id")
    .limit(5)
)

from pyspark.sql import functions as F

prepared_adjusters = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.lower(F.col("adjuster_name")).like("% s%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.concat_ws(" / ", F.col("adjuster_name"), F.col("team")).alias("adjuster_name_team_label"),
        F.when(F.col("level") >= 3, "large").when(F.col("level") >= 2, "medium").otherwise("small").alias("level_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("reported_at").alias("first_claim_reported_at_2"),
    )
)

with_counts = (
    prepared_adjusters.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_reported_at",
        "c.first_claim_reported_at_2",
    )
)

enriched = (
    with_counts
    .filter(F.col("claim_count") < 7)
    .select(
        "*",
        F.year(F.col("first_claim_reported_at")).alias("first_claim_reported_at_year"),
        F.to_date(F.col("first_claim_reported_at")).alias("first_claim_reported_at_date"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_type") == "delay"), "left_semi")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left_semi")
)

screened = (
    with_activity_2.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2025-06-24").cast("date")) & (F.col("c.claim_type") == "delay"), "left_anti")
)

scored = (
    screened
    .select(
        "*",
        F.when(F.col("level_tier").isin("large", "medium"), "group_a").when(F.col("level_tier") == "small", "group_b").otherwise("other").alias("level_tier_group"),
        F.length(F.trim(F.col("level_tier"))).alias("level_tier_len"),
    )
)

result = (
    scored
    .select(
        "id",
        "adjuster_name",
        "team",
        "adjuster_name_team_label",
        "level_tier",
        "claim_count",
        "first_claim_reported_at",
        "first_claim_reported_at_year",
        "first_claim_reported_at_date",
        "level_tier_len",
        F.abs(F.col("level") - 3).alias("level_distance"),
        (F.col("first_claim_reported_at") + F.expr("INTERVAL 2 HOURS")).alias("first_claim_reported_at_plus_2h"),
    )
    .orderBy(
        F.col("adjuster_name").desc(),
        F.col("first_claim_reported_at_year").asc_nulls_last(),
        "id",
    )
    .limit(20)
)

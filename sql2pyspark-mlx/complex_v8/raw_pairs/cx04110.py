from pyspark.sql import functions as F

base = (
    df_adjusters
    .filter((F.col("level") >= 2) & (F.col("adjuster_name").isNotNull()))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.length(F.trim(F.col("team"))).alias("team_len_2"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

annotated = (
    base
    .filter(F.col("team_len_2").between(6, 10))
    .withColumn("level_distance", F.abs(F.col("level") - 3))
)

team_or_unknown_stats = (
    annotated
    .groupBy("team_or_unknown")
    .agg(F.percentile_approx("team_len_2", 0.5).alias("median_team_len_2"))
)

compared = (
    annotated.alias("s")
    .join(team_or_unknown_stats.alias("g"), F.col("g.team_or_unknown") == F.col("s.team_or_unknown"), "inner")
    .filter(F.col("s.team_len_2") >= F.col("g.median_team_len_2"))
    .select(
        "s.*",
        "g.median_team_len_2",
        F.round(F.col("s.team_len_2") - F.col("g.median_team_len_2"), 2).alias("team_len_2_minus_median"),
    )
)

scored = (
    compared
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "level",
        "team_len_2",
        "team_or_unknown",
        "team_len_2_minus_median",
        F.coalesce(F.col("team_or_unknown"), F.lit("UNKNOWN")).alias("team_or_unknown_or_unknown"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count("*").alias("claim_count_2"),
    )
)

profiled = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

derived = (
    profiled
    .select(
        "*",
        F.when(F.col("level") >= 3, "large").when(F.col("level") >= 1, "medium").otherwise("small").alias("level_tier"),
        F.length(F.trim(F.col("team_or_unknown"))).alias("team_or_unknown_len"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at_2"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.count(F.when(F.col("claim_type").isin("delay", "fire"), 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    derived.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at_2",
        "c.max_claim_claimed_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "team_len_2",
        "level",
        "claim_count",
        "claim_claim_type_count",
    )
    .unpivot(
        ids=["id"],
        values=["team_len_2", "level", "claim_count", "claim_claim_type_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)

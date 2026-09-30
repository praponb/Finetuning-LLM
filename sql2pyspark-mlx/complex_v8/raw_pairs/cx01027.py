from pyspark.sql import functions as F

base_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

summary = (
    base_agents
    .filter(F.col("hired_on") >= F.lit("2024-05-02").cast("date"))
    .groupBy(
        "tier_or_unknown",
        F.date_format("hired_on", "yyyy-MM").alias("hired_on_ym"),
        "team",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("tier_or_unknown").alias("distinct_tier_or_unknowns"),
        F.min("hired_on").alias("first_hired_on"),
        F.countDistinct("tier").alias("distinct_tiers"),
    )
)

annotated = (
    summary
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tier_or_unknown")), " ", "_")).alias("tier_or_unknown_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_hired_on")).alias("days_since_first_hired_on"),
        F.lower(F.regexp_replace(F.trim(F.col("tier_or_unknown")), " ", "_")).alias("tier_or_unknown_slug_2"),
    )
)

result = (
    annotated
    .filter(F.col("days_since_first_hired_on") > 210)
    .select(
        "tier_or_unknown",
        "hired_on_ym",
        "team",
        "row_count",
        "distinct_tier_or_unknowns",
        "first_hired_on",
        "distinct_tiers",
        "days_since_first_hired_on",
        "tier_or_unknown_slug_2",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

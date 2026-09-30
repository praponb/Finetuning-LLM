from pyspark.sql import functions as F

recent = (
    df_support_agents
    .filter((F.col("hired_on").between(F.lit("2024-04-14").cast("date"), F.lit("2025-02-27").cast("date"))) | (F.upper(F.trim(F.col("agent_name"))).like("I%")))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
        F.when(F.col("team").isin("Billing", "Tech"), "group_a").when(F.col("team") == "VIP", "group_b").otherwise("other").alias("team_group"),
    )
)

qualified = (
    recent
    .crossJoin(recent.agg(F.avg("days_since_hired_on").alias("overall_avg_days_since_hired_on")))
    .filter(F.col("days_since_hired_on") > F.col("overall_avg_days_since_hired_on"))
    .drop("overall_avg_days_since_hired_on")
)

stats = (
    qualified
    .groupBy("team_group")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("tier").alias("distinct_tiers"),
    )
)

result = (
    stats
    .cube("team_group", "row_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("distinct_tiers"), 2).alias("stddev_distinct_tiers"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("team_group").asc_nulls_last(),
        F.col("row_count").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F

filtered = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-27").cast("date")) & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("team").isin("Billing", "Tech"), "group_a").when(F.col("team") == "Onboarding", "group_b").otherwise("other").alias("team_group"),
        F.month(F.col("hired_on")).alias("hired_on_month"),
    )
)

derived = (
    filtered
    .select(
        "*",
        F.substring(F.trim(F.col("agent_name")), 1, 4).alias("agent_name_prefix"),
        F.year(F.col("hired_on")).alias("hired_on_year"),
        F.abs(F.col("hired_on_month") - 9).alias("hired_on_month_distance"),
    )
)

result = (
    derived
    .rollup("tier", "team_group", "hired_on_month")
    .agg(
        F.max("hired_on").alias("last_hired_on"),
        F.round(F.stddev("hired_on_year"), 2).alias("stddev_hired_on_year"),
        F.round(F.avg("hired_on_month_distance"), 2).alias("avg_hired_on_month_distance"),
    )
    .orderBy(
        F.col("tier").asc_nulls_last(),
        F.col("team_group").asc_nulls_last(),
        F.col("hired_on_month").asc_nulls_last(),
    )
)

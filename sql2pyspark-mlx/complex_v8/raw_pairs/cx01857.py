from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter((F.col("hired_on") >= F.lit("2024-07-09").cast("date")) & (~F.col("tier").isin("T2", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
    )
)

scored = (
    valid_agents
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.date_trunc("week", F.col("hired_on")).alias("hired_on_week"),
    )
)

result = (
    scored
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        "hired_on_week",
        F.month(F.col("hired_on_week")).alias("hired_on_week_month"),
        F.substring(F.trim(F.col("team")), 1, 2).alias("team_prefix"),
    )
    .orderBy("team")
)

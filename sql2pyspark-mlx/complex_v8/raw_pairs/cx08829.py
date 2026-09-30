from pyspark.sql import functions as F

cleaned_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

agent_rows = (
    cleaned_agents
    .filter((F.col("tier") == "T2") & (F.col("team") == "Onboarding"))
    .select("id")
)

agent_rows_2 = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2024-12-31").cast("date"))
    .select("id")
)

result = (
    agent_rows
    .subtract(agent_rows_2)
    .orderBy("id")
)

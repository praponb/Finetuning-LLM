from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter(F.col("team") == "VIP")
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
    )
)

agent_rows = (
    scoped
    .filter((F.col("hired_on") < F.lit("2024-12-31").cast("date")) & (~F.col("team").isin("Billing", "Onboarding")))
    .select("id")
)

agent_rows_2 = df_support_agents.select("id")

result = (
    agent_rows
    .intersectAll(agent_rows_2)
    .orderBy(F.col("id").desc())
)

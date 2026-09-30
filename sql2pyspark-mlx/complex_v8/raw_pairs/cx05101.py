from pyspark.sql import functions as F

cleaned_agents = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("% n%"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
    )
)

agent_side = (
    cleaned_agents
    .filter(F.col("tier").isin("T2", "T1"))
    .select("team")
)

agent_rows = (
    df_support_agents
    .filter(F.col("tier") == "T2")
    .select("team")
)

result = (
    agent_side
    .subtract(agent_rows)
    .orderBy("team")
)

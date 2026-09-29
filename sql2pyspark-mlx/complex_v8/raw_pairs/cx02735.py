from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        F.col("hired_on").alias("event_date"),
        F.col("agent_name").alias("name"),
    )
)

agent_side_2 = (
    df_support_agents
    .filter(F.year(F.col("hired_on")) == 2025)
    .select("id", "hired_on", "agent_name")
)

result = agent_side.union(agent_side_2)

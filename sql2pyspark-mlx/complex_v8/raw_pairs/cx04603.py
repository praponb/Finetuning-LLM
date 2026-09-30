from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .select(
        "tier",
        F.col("agent_name").alias("name"),
        "hired_on",
        F.lit("agent").alias("source"),
    )
)

agent_rows = (
    df_support_agents
    .select(
        "team",
        "tier",
        "hired_on",
        F.lit("agent_2").alias("source"),
    )
)

result = agent_side.union(agent_rows)

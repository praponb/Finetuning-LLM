from pyspark.sql import functions as F

valid = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.date_trunc("week", F.col("hired_on")).alias("hired_on_week"),
    )
)

agent_side = (
    valid
    .filter((F.upper(F.trim(F.col("agent_name"))).like("J%")) & (~F.col("tier").isin("T3", "T1")))
    .select(
        "tier",
        "id",
        "hired_on",
        F.lit("agent").alias("src"),
    )
)

agent_side_2 = (
    df_support_agents
    .select(
        F.col("agent_name").alias("tier"),
        "id",
        "hired_on",
        F.lit("agent_2").alias("src"),
    )
)

result = agent_side.union(agent_side_2)

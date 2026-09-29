from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .select(
        F.col("id").alias("ref_id"),
        F.col("hired_on").alias("event_date"),
        F.col("team").alias("name"),
    )
)

agent_side_2 = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("H%"))
    .select("id", "hired_on", "tier")
)

agent_side_3 = (
    df_support_agents
    .filter((F.col("tier").isin("T1", "T3")) & (F.upper(F.trim(F.col("agent_name"))).like("I%")))
    .select("id", "hired_on", "agent_name")
)

result = (
    agent_side
    .union(agent_side_2)
    .union(agent_side_3)
    .orderBy(F.col("event_date").desc())
)

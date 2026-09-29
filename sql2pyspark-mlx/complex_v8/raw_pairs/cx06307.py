from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(F.col("event_type").alias("label"), "id", F.lit("event").alias("src"))
)

agent_side = (
    df_support_agents
    .filter(F.year(F.col("hired_on")) == 2025)
    .select(F.col("agent_name").alias("label"), "id", F.lit("agent").alias("src"))
)

result = event_side.unionByName(agent_side)

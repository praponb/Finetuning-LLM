from pyspark.sql import functions as F

event_side = df_ticket_events.select(F.col("actor").alias("name"))

agent_side = df_support_agents.select("agent_name")

result = (
    event_side
    .exceptAll(agent_side)
    .orderBy(F.col("name").desc())
)

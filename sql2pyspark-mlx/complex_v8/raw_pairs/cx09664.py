from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select("ticket_id", F.col("event_type").alias("name"))
)

agent_side = (
    df_support_agents
    .filter(F.col("agent_name").isNotNull())
    .select("id", "team")
)

result = (
    event_side
    .subtract(agent_side)
    .orderBy(F.col("name").desc())
)

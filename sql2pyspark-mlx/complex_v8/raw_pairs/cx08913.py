from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("ticket_id").isNotNull()))
    .select(F.col("ticket_id").alias("key_id"))
)

agent_side = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("I%"))
    .select(F.col("id").alias("key_id"))
)

result = event_side.subtract(agent_side)

from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isNotNull()))
    .select(F.col("ticket_id").alias("qty"), "event_type")
)

agent_side = df_support_agents.select("id", "agent_name")

result = event_side.intersectAll(agent_side)

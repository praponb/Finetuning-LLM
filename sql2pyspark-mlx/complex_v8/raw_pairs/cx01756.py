from pyspark.sql import functions as F

agent_rows = df_support_agents.select(F.col("id").alias("key_id"))

event_side = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("event_type").isNotNull()))
    .select(F.col("ticket_id").alias("key_id"))
)

result = agent_rows.intersect(event_side)

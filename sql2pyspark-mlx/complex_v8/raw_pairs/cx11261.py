from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select("actor")
)

agent_side = df_support_agents.select(F.col("tier").alias("actor"))

result = (
    event_side
    .intersect(agent_side)
    .orderBy("actor")
)

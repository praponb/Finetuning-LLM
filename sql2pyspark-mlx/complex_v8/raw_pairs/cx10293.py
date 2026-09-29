from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type").isin("replied", "escalated")) & (F.col("ticket_id") <= 11))
    .select("actor")
)

agent_side = (
    df_support_agents
    .filter((F.col("team").isin("Tech", "VIP")) & (F.length(F.trim(F.col("agent_name"))) >= 4))
    .select("agent_name")
)

result = event_side.intersectAll(agent_side)

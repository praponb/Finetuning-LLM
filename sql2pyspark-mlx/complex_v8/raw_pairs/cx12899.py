from pyspark.sql import functions as F

recent = df_ticket_events.filter(F.col("ticket_id") >= 45)

event_rows = (
    recent
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") != "replied"))
    .select("actor", "id")
)

agent_rows = (
    df_support_agents
    .filter(F.col("team").isNotNull())
    .select("agent_name", "id")
)

result = (
    event_rows
    .union(agent_rows)
    .distinct()
    .orderBy("id")
)

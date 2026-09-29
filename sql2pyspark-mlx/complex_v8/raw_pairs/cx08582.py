from pyspark.sql import functions as F

agent_side = df_support_agents.select("id", "tier")

ticket_rows = df_tickets.select("requester_id", "channel")

result = (
    agent_side
    .intersect(ticket_rows)
    .orderBy("id")
)

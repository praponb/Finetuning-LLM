from pyspark.sql import functions as F

ticket_rows = df_tickets.select("priority")

agent_side = df_support_agents.select("tier")

result = (
    ticket_rows
    .intersect(agent_side)
    .orderBy("priority")
)

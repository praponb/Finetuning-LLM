from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("H%"))
    .select("id")
)

ticket_side = (
    df_tickets
    .filter(F.col("requester_id") <= 19)
    .select("csat")
)

result = (
    agent_rows
    .subtract(ticket_side)
    .orderBy(F.col("id").desc())
)

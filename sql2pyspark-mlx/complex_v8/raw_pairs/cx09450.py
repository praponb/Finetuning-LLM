from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.col("tier") == "T2")
    .select("agent_name")
)

ticket_rows = (
    df_tickets
    .filter((F.col("requester_id") <= 23) & (F.col("channel") == "web"))
    .select(F.col("channel").alias("agent_name"))
)

result = (
    agent_rows
    .subtract(ticket_rows)
    .orderBy("agent_name")
)

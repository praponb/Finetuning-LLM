from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("F%")) & (F.col("hired_on") < F.lit("2025-02-27").cast("date")))
    .select("id")
)

ticket_rows = df_tickets.select("requester_id")

result = (
    agent_side
    .subtract(ticket_rows)
    .orderBy(F.col("id").desc())
)

from pyspark.sql import functions as F

agent_rows = df_support_agents.select("id")

event_rows = (
    df_ticket_events
    .filter(~F.col("actor").isin("agent", "customer"))
    .select("ticket_id")
)

result = (
    agent_rows
    .intersectAll(event_rows)
    .orderBy("id")
)

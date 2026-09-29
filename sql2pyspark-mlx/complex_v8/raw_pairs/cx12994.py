from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 27) & (F.col("actor").isin("customer", "agent")))
    .select("id", "actor")
)

agent_side = (
    df_support_agents
    .filter(F.col("tier") != "T2")
    .select("id", F.col("tier").alias("actor"))
)

result = event_rows.subtract(agent_side)

from pyspark.sql import functions as F

event_side = df_ticket_events.select(F.col("ticket_id").alias("qty"))

agent_rows = (
    df_support_agents
    .filter((~F.col("tier").isin("T3", "T2")) & (F.length(F.trim(F.col("agent_name"))) >= 6))
    .select("id")
)

result = event_side.subtract(agent_rows)

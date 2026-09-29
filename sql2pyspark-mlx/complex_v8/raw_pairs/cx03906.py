from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 57) & (F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select("ticket_id")
)

agent_side = df_support_agents.select(F.col("id").alias("ticket_id"))

result = (
    event_rows
    .exceptAll(agent_side)
    .orderBy("ticket_id")
)

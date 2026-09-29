from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.month(F.col("event_at")).isin(2, 3)))
    .select(F.col("id").alias("ref_id"), "actor")
)

agent_rows = df_support_agents.select("id", "agent_name")

result = event_rows.intersectAll(agent_rows)

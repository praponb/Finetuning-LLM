from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type").isin("created", "assigned")) & (F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")))
    .select(F.col("ticket_id").alias("qty"), "event_type")
)

agent_side = (
    df_support_agents
    .filter(F.col("team") == "VIP")
    .select(F.col("id").alias("qty"), F.col("agent_name").alias("event_type"))
)

result = (
    event_side
    .intersect(agent_side)
    .orderBy(F.col("qty").desc(), "event_type")
)

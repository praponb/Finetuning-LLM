from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-27").cast("date")) & (F.col("team").isNotNull()))
    .select(F.col("id").alias("key_id"))
)

event_side = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("actor").isin("system", "agent")))
    .select("id")
)

result = agent_side.intersect(event_side)

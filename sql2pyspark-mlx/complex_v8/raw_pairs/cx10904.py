from pyspark.sql import functions as F

event_side = df_ticket_events.select("id")

agent_side = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .select("id")
)

result = event_side.intersect(agent_side)

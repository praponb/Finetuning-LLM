from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") >= F.lit("2024-07-09").cast("date")) & (F.upper(F.trim(F.col("agent_name"))).like("H%")))
    .select(F.col("id").alias("qty"))
)

event_rows = df_ticket_events.select(F.col("id").alias("qty"))

result = (
    agent_side
    .intersect(event_rows)
    .orderBy("qty")
)

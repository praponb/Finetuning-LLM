from pyspark.sql import functions as F

agent_side = df_support_agents.select(F.col("team").alias("label"))

event_side = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "customer")) & (F.col("ticket_id").isNotNull()))
    .select(F.col("event_type").alias("label"))
)

result = (
    agent_side
    .subtract(event_side)
    .orderBy(F.col("label").desc())
)

from pyspark.sql import functions as F

agent_rows = df_support_agents.select(F.col("id").alias("qty"), F.col("team").alias("category"))

event_side = (
    df_ticket_events
    .select(F.col("ticket_id").alias("qty"), F.col("actor").alias("category"))
)

result = (
    agent_rows
    .subtract(event_side)
    .orderBy(F.col("qty").desc())
)

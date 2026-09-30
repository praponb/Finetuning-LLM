from pyspark.sql import functions as F

event_side = df_ticket_events.select(F.col("event_type").alias("category"))

agent_side = (
    df_support_agents
    .filter(F.col("team").isin("Onboarding", "Tech"))
    .select("agent_name")
)

result = (
    event_side
    .exceptAll(agent_side)
    .orderBy(F.col("category").desc())
)

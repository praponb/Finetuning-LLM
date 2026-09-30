from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2024-12-31").cast("date")) & (F.col("tier") != "T2"))
    .select("id")
)

event_side = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "customer"))
    .select(F.col("ticket_id").alias("id"))
)

result = (
    agent_side
    .subtract(event_side)
    .orderBy(F.col("id").desc())
)

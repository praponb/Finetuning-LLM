from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("tier") != "T3") & (F.col("team") != "Tech"))
    .select(F.col("id").alias("qty"))
)

event_rows = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select("ticket_id")
)

result = (
    agent_side
    .subtract(event_rows)
    .orderBy(F.col("qty").desc())
)

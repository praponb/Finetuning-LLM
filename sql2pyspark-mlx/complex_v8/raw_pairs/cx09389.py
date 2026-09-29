from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.col("tier") == "T3")
    .select("team", F.col("id").alias("entity_id"))
)

ticket_side = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 3))
    .select(F.col("priority").alias("team"), F.col("agent_id").alias("entity_id"))
)

result = (
    agent_rows
    .unionByName(ticket_side)
    .orderBy(F.col("team").desc())
)

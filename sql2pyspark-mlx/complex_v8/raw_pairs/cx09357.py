from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("requester_id") <= 16)
    .select("requester_id")
)

agent_rows = (
    df_support_agents
    .filter(F.col("tier").isNotNull())
    .select(F.col("id").alias("requester_id"))
)

result = (
    ticket_side
    .exceptAll(agent_rows)
    .orderBy("requester_id")
)

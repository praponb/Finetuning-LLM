from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("tier") == "T2"))
    .select("id")
)

ticket_side = (
    df_tickets
    .filter((~F.col("channel").isin("web", "email")) & (F.col("priority") == "P2"))
    .select("id")
)

result = (
    agent_side
    .exceptAll(ticket_side)
    .orderBy("id")
)

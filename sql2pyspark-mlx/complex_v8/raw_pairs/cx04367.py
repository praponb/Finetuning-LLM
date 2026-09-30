from pyspark.sql import functions as F

ticket_side = df_tickets.select("id")

agent_side = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("tier") == "T1"))
    .select("id")
)

result = (
    ticket_side
    .intersect(agent_side)
    .orderBy(F.col("id").desc())
)

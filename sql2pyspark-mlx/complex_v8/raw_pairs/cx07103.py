from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("team") == "Onboarding") & (F.col("tier") == "T3"))
    .select("tier")
)

ticket_side = df_tickets.select("priority")

result = (
    agent_side
    .exceptAll(ticket_side)
    .orderBy(F.col("tier").desc())
)

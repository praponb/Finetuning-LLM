from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter((F.col("team").isin("VIP", "Onboarding")) & (F.col("hired_on").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

agent_rows = scoped.select("tier")

ticket_side = df_tickets.select(F.col("channel").alias("tier"))

result = agent_rows.intersect(ticket_side)

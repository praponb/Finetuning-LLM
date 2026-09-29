from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 2) | (F.col("requester_id") < 23))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
    )
)

ticket_side = (
    df_tickets
    .filter((F.col("csat") < 4) & (F.col("requester_id") > 11))
    .select(F.col("channel").alias("name"))
)

agent_side = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%ha%")) & (F.col("team").isin("VIP", "Billing", "Onboarding")))
    .select("team")
)

result = (
    ticket_side
    .intersect(agent_side)
    .orderBy("name")
)

from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(1, 3)) & (F.col("requester_id") <= 19))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
    )
)

ticket_side = cleaned_tickets.select("id")

agent_side = df_support_agents.select("id")

result = (
    ticket_side
    .intersect(agent_side)
    .orderBy("id")
)

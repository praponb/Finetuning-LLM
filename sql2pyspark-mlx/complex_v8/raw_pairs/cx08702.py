from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) & (F.col("priority") != "P1"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

ticket_rows = scoped_tickets.select(F.col("agent_id").alias("qty"))

agent_rows = df_support_agents.select(F.col("id").alias("qty"))

result = ticket_rows.exceptAll(agent_rows)

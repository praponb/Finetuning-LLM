from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id") <= 8) & (~F.col("channel").isin("email", "phone")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

ticket_side = (
    scoped_tickets
    .filter(F.col("agent_id") >= 4)
    .select(F.col("id").alias("entity_id"))
)

event_side = df_ticket_events.select(F.col("id").alias("entity_id"))

result = (
    ticket_side
    .subtract(event_side)
    .orderBy(F.col("entity_id").desc())
)

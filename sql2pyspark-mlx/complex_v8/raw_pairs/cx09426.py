from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(9, 11)) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
    )
)

ticket_side = (
    prepared_tickets
    .filter(F.col("channel") == "email")
    .select("id", "agent_id")
)

ticket_rows = df_tickets.select("requester_id", "id")

ticket_side_2 = df_tickets.select("requester_id", "id")

result = (
    ticket_side
    .union(ticket_rows)
    .union(ticket_side_2)
)

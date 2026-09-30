from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.array_contains(F.col("tags"), "login").alias("has_login"),
    )
)

ticket_rows = scoped_tickets.select("id")

event_side = (
    df_ticket_events
    .filter((F.col("actor") != "system") & (F.col("ticket_id").isNotNull()))
    .select("id")
)

result = (
    ticket_rows
    .intersect(event_side)
    .orderBy(F.col("id").desc())
)

from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("priority").isin("P1", "P4")) & (F.col("csat") >= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
    )
)

ticket_side = (
    base_tickets
    .filter((F.col("csat") <= 4) & (F.col("requester_id") <= 20))
    .select(F.col("csat").alias("ref_id"))
)

event_side = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select("id")
)

result = (
    ticket_side
    .subtract(event_side)
    .orderBy("ref_id")
)

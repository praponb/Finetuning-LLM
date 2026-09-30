from pyspark.sql import functions as F

valid = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.when(F.col("requester_id") >= 21, "large").when(F.col("requester_id") >= 8, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

ticket_rows = valid.select(F.col("requester_id").alias("ref_id"), "agent_id")

event_side = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.col("ticket_id").isNotNull()))
    .select(F.col("ticket_id").alias("ref_id"), F.col("id").alias("agent_id"))
)

ticket_rows_2 = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) & (F.col("csat").between(2, 4)))
    .select(F.col("requester_id").alias("ref_id"), "agent_id")
)

result = (
    ticket_rows
    .unionByName(event_side)
    .unionByName(ticket_rows_2)
    .orderBy(F.col("ref_id").desc())
)

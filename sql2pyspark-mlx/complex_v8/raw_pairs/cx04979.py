from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 54) & (F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

event_rows = valid.select(F.col("ticket_id").alias("ref_id"), "event_type")

ticket_rows = (
    df_tickets
    .filter(F.col("channel") == "web")
    .select("requester_id", "priority")
)

result = (
    event_rows
    .union(ticket_rows)
    .orderBy(F.col("ref_id").desc(), "event_type")
)

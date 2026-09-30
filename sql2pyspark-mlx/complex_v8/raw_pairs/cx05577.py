from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_rows = (
    filtered
    .filter((~F.col("event_type").isin("assigned", "created")) & (F.month(F.col("event_at")).isin(1, 2)))
    .select(F.col("id").alias("key_id"), "actor")
)

ticket_side = (
    df_tickets
    .filter((~F.col("channel").isin("web", "chat")) & (F.col("csat") >= 2))
    .select("id", "channel")
)

result = event_rows.intersect(ticket_side)

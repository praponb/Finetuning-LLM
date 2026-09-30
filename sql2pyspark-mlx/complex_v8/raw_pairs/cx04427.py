from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_parent_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    with_parent_ticket
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_opened_at",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
    .orderBy(F.col("actor").desc(), "id")
)

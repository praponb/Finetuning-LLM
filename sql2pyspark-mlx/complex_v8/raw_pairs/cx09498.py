from pyspark.sql import functions as F

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

exploded_detail_tickets = (
    in_window.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") <= 17), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    exploded_detail_tickets
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "agent_id",
        "hours_since_open",
        "ticket_requester_id",
        "ticket_closed_at",
    )
    .orderBy(F.col("hours_since_open").desc())
)

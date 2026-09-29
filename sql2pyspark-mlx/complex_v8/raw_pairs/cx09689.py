from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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
    matched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.closed_at")) < F.lit("2025-02-08").cast("date")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

filtered = (
    exploded_detail_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "created"), "left_semi")
)

combined_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.ticket_id_2",
        "s.ticket_closed_at",
        "s.ticket_priority",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    combined_details_ticket
    .filter(F.array_contains(F.col("ticket_tags"), "bug"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "ticket_closed_at",
        "ticket_priority",
        "ticket_tags",
        F.array_join(F.array_sort(F.col("ticket_tags")), ",").alias("ticket_tags_text"),
    )
)

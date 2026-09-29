from pyspark.sql import functions as F

matched = (
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

detail_rows_tickets = (
    matched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isin("P1", "P3")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    detail_rows_tickets
    .select(
        "id",
        "ticket_id",
        "priority",
        "ticket_priority",
        "ticket_tags",
        F.substring(F.trim(F.col("ticket_priority")), 1, 3).alias("ticket_priority_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
    )
)

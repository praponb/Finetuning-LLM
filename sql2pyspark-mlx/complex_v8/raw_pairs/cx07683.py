from pyspark.sql import functions as F

in_window = (
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

enriched = (
    in_window
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.days_since_event_at",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

scored = (
    detail_rows
    .withColumn("ticket_closed_at_label", F.date_format(F.col("ticket_closed_at"), "MMM yyyy"))
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "priority",
        "agent_id",
        "days_since_event_at",
        "ticket_id_2",
        "ticket_closed_at",
        "ticket_channel",
        "ticket_closed_at_label",
        F.concat_ws("-", F.col("ticket_channel"), F.col("priority")).alias("ticket_channel_priority_label"),
    )
    .orderBy(
        F.col("ticket_channel").desc(),
        F.col("ticket_channel_priority_label").desc(),
    )
)

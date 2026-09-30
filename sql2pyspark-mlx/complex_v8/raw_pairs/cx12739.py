from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type") != "resolved") | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(3, 18)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

ticket_id_stats = (
    with_details_ticket
    .groupBy("ticket_id")
    .agg(F.max("ticket_csat").alias("max_ticket_csat"))
)

compared = (
    with_details_ticket.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.ticket_csat") == F.col("g.max_ticket_csat"))
    .select(
        "s.*",
        "g.max_ticket_csat",
        F.round(F.col("s.ticket_csat") - F.col("g.max_ticket_csat"), 2).alias("ticket_csat_minus_max"),
    )
)

result = (
    compared
    .groupBy("ticket_agent_id", "ticket_id")
    .pivot("ticket_channel", ["email", "phone"])
    .agg(F.min("max_ticket_csat"))
)

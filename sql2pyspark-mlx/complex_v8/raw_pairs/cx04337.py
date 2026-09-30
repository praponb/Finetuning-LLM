from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

enriched_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_slug",
        "s.event_at_week",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

combined_details_ticket = (
    enriched_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_slug",
        "s.event_at_week",
        "s.ticket_closed_at",
        F.col("t.csat").alias("ticket_csat_2"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_activity = (
    combined_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left_anti")
)

without_matches = (
    with_activity
    .crossJoin(with_activity.agg(F.max("ticket_csat_2").alias("overall_max_ticket_csat_2")))
    .filter(F.col("ticket_csat_2") < F.col("overall_max_ticket_csat_2"))
    .drop("overall_max_ticket_csat_2")
)

enriched_ticket_2 = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.size(F.col("t.tags")) >= 2), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_slug",
        "s.event_at_week",
        "s.ticket_closed_at",
        "s.ticket_csat_2",
        "s.ticket_closed_at_2",
        "s.ticket_requester_id",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    enriched_ticket_2
    .select(
        "id",
        "event_at",
        "ticket_closed_at_2",
        "ticket_requester_id",
        "ticket_csat",
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("id").cast("string")).alias("ticket_channel_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

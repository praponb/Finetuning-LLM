from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 23)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_parent_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_at_plus_2h",
        "s.event_type_clean",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_details_ticket = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_at_plus_2h",
        "s.ticket_opened_at",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_details_ticket
    .cube("ticket_channel", "event_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("ticket_channel")).alias("ticket_channel_list"),
    )
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)

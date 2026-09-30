from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

combined_details_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_or_unknown",
        "s.actor_group",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    combined_details_ticket
    .groupingSets(
        [["ticket_priority", "ticket_id"], ["ticket_priority"]],
        "ticket_priority", "ticket_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("ticket_requester_id").alias("max_ticket_requester_id"),
    )
    .orderBy(
        F.col("ticket_priority").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)

from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("event_type") != "resolved") & (F.col("ticket_id") >= 50))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "reopened"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

joined = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_details_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.ticket_priority",
        "s.ticket_requester_id",
        "s.ticket_opened_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

scored = (
    with_details_ticket
    .filter(F.col("ticket_requester_id") > 11)
    .select(
        "id",
        "event_type",
        "actor",
        "ticket_priority",
        "ticket_requester_id",
        "ticket_opened_at",
        "ticket_agent_id",
        F.to_date(F.col("ticket_opened_at")).alias("ticket_opened_at_date"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.sum("csat").alias("total_ticket_csat"))
)

with_counts = (
    scored.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select("s.*", "t.total_ticket_csat")
)

result = (
    with_counts
    .select(
        "id",
        "event_type",
        "actor",
        "ticket_priority",
        "ticket_requester_id",
        "ticket_opened_at",
        "ticket_opened_at_date",
        "total_ticket_csat",
        F.date_trunc("month", F.col("ticket_opened_at_date")).alias("ticket_opened_at_date_month"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
    .orderBy(F.col("id").desc(), F.col("event_type").desc())
)

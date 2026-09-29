from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .filter(F.col("team").isin("VIP", "Tech", "Onboarding"))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.hired_on",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    with_children.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

ticket_channel_stats = (
    with_counts
    .groupBy("ticket_channel")
    .agg(F.percentile_approx("ticket_requester_id", 0.5).alias("median_ticket_requester_id"))
)

compared = (
    with_counts.alias("s")
    .join(ticket_channel_stats.alias("g"), F.col("g.ticket_channel") == F.col("s.ticket_channel"), "inner")
    .filter(F.col("s.ticket_requester_id") >= F.col("g.median_ticket_requester_id"))
    .select(
        "s.*",
        "g.median_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.median_ticket_requester_id"), 2).alias("ticket_requester_id_minus_median"),
    )
)

with_details = (
    compared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.hired_on",
        "s.ticket_id",
        "s.ticket_priority",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.ticket_requester_id_minus_median",
        F.col("t.priority").alias("ticket_priority_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_activity = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 4), "left_semi")
)

qualified_2 = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (F.col("t.event_type") == "created"), "left_semi")
)

result = (
    qualified_2
    .filter(F.col("ticket_requester_id_minus_median") >= -12.5)
    .select(
        "id",
        "hired_on",
        "ticket_id",
        "ticket_channel",
        "ticket_requester_id_minus_median",
        "ticket_priority_2",
    )
    .orderBy(
        F.col("ticket_requester_id_minus_median").desc(),
        F.col("hired_on").desc_nulls_first(),
    )
)

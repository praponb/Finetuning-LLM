from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.closed_at")).isin(2, 3)), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

enriched_ticket = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_details_ticket = (
    enriched_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        "s.ticket_requester_id",
        F.col("t.csat").alias("ticket_csat_2"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

event_type_stats = (
    with_details_ticket
    .groupBy("event_type")
    .agg(F.avg("ticket_csat_2").alias("avg_ticket_csat_2"))
)

compared = (
    with_details_ticket.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.ticket_csat_2") > F.col("g.avg_ticket_csat_2"))
    .select(
        "s.*",
        "g.avg_ticket_csat_2",
        F.round(F.col("s.ticket_csat_2") - F.col("g.avg_ticket_csat_2"), 2).alias("ticket_csat_2_minus_avg"),
    )
)

result = (
    compared
    .filter(F.col("ticket_csat_2") <= 3)
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_actor_label",
        "ticket_requester_id",
        "ticket_csat_2",
        "ticket_tags",
        "ticket_agent_id",
        "avg_ticket_csat_2",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
    .orderBy("ticket_requester_id", "id")
    .limit(10)
)

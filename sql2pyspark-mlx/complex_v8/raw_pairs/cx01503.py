from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 43)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

annotated = (
    prepared_events
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_ref")), " ", "_")).alias("event_type_ref_slug"),
        F.when(F.col("actor_or_unknown").isin("system", "UNKNOWN"), "group_a").when(F.col("actor_or_unknown") == "agent", "group_b").otherwise("other").alias("actor_or_unknown_group"),
    )
)

enriched_ticket = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_type_prefix",
        "s.event_type_ref_slug",
        "s.actor_or_unknown_group",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

joined = (
    enriched_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") <= 5), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_type_prefix",
        "s.event_type_ref_slug",
        "s.ticket_priority",
        F.col("t.priority").alias("ticket_priority_2"),
    )
)

joined_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.closed_at")).between(7, 9)), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_type_prefix",
        "s.event_type_ref_slug",
        "s.ticket_priority",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

qualified = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system") & (F.col("t.event_type").isin("created", "replied", "resolved")), "left_anti")
    .filter(F.col("actor_or_unknown").isin("system", "UNKNOWN"))
)

result = (
    qualified
    .filter(F.col("ticket_priority") == "P4")
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_prefix",
        "event_type_ref_slug",
        "ticket_priority",
        "agent_tier",
        "agent_name",
        F.when(F.col("ticket_requester_id") >= 20, "A").when(F.col("ticket_requester_id") >= 9, "B").otherwise("C").alias("ticket_requester_id_tier"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("ticket_id").desc())
)

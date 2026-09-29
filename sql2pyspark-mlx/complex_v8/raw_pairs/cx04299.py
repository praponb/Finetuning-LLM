from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("agent_id") >= 8) & (F.col("requester_id") <= 20))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("requester_id").alias("max_ticket_requester_id"))
)

summarized_rows = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.max_ticket_requester_id")
)

agent_id_stats = (
    summarized_rows
    .groupBy("agent_id")
    .agg(F.max("requester_id").alias("max_requester_id"))
)

compared = (
    summarized_rows.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") == F.col("g.max_requester_id"))
    .select(
        "s.*",
        "g.max_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.max_requester_id"), 2).alias("requester_id_minus_max"),
    )
)

with_children_events = (
    compared.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "system"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat_per_requester_id",
        "s.max_ticket_requester_id",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

without_matches = (
    with_children_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "reopened"), "left_semi")
    .filter(F.col("priority").isin("P3", "P1"))
)

line_level = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.max_ticket_requester_id",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

annotated = (
    line_level
    .filter(F.col("event_actor") != "agent")
    .select(
        "id",
        "max_ticket_requester_id",
        "event_id",
        "event_at",
        "event_actor",
        "event_id_2",
        "event_actor_2",
        "event_event_type",
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        F.when(F.col("event_actor").isin("customer", "agent"), "group_a").when(F.col("event_actor") == "system", "group_b").otherwise("other").alias("event_actor_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        "max_ticket_requester_id",
        "event_id",
        "event_at",
        "event_id_2",
        "event_actor_2",
        "event_event_type",
        "opened_at_label",
        "event_actor_group",
        F.coalesce(F.col("event_event_type"), F.lit("UNKNOWN")).alias("event_event_type_or_unknown"),
    )
    .orderBy("id")
)

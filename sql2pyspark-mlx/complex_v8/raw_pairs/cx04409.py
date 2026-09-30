from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.when(F.col("event_type").isin("reopened", "escalated"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

with_details_ticket = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") < 23), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_prefix",
        "s.event_type_group",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_details_ticket
    .select(
        "*",
        F.month(F.col("agent_hired_on")).alias("agent_hired_on_month"),
        F.date_format(F.col("ticket_closed_at"), "yyyy-MM").alias("ticket_closed_at_label"),
    )
)

exploded = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_prefix",
        "event_type_group",
        "ticket_closed_at",
        "agent_name",
        "agent_hired_on",
        "agent_hired_on_month",
        "ticket_closed_at_label",
        F.explode("ticket_tags").alias("ticket_tag"),
    )
)

event_type_stats = (
    exploded
    .groupBy("event_type")
    .agg(F.max("agent_hired_on_month").alias("max_agent_hired_on_month"))
)

vs_group = (
    exploded.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.agent_hired_on_month") == F.col("g.max_agent_hired_on_month"))
    .select(
        "s.*",
        "g.max_agent_hired_on_month",
        F.round(F.col("s.agent_hired_on_month") - F.col("g.max_agent_hired_on_month"), 2).alias("agent_hired_on_month_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "actor",
        "actor_prefix",
        "event_type_group",
        "agent_name",
        "agent_hired_on",
        "agent_hired_on_month",
        "ticket_closed_at_label",
        "ticket_tag",
        "max_agent_hired_on_month",
        "agent_hired_on_month_minus_max",
        F.year(F.col("ticket_closed_at")).alias("ticket_closed_at_year"),
        F.coalesce(F.col("agent_hired_on_month"), F.lit(0)).alias("agent_hired_on_month_filled"),
    )
    .orderBy(F.col("agent_hired_on_month_filled").asc_nulls_last())
)

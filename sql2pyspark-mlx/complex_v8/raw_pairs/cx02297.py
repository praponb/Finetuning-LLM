from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

event_type_stats = (
    base
    .groupBy("event_type")
    .agg(F.avg("event_at_hour").alias("avg_event_at_hour"))
)

above_average = (
    base.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_hour") > F.col("g.avg_event_at_hour"))
    .select(
        "s.*",
        "g.avg_event_at_hour",
        F.round(F.col("s.event_at_hour") - F.col("g.avg_event_at_hour"), 2).alias("event_at_hour_minus_avg"),
    )
)

with_parent_ticket = (
    above_average.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_parent_ticket_2 = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.priority").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_prefix",
        "s.event_at_hour",
        "s.event_at_hour_minus_avg",
        "s.ticket_tags",
        "s.ticket_csat",
        "s.agent_tier",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

line_level_events = (
    with_parent_ticket_2.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_prefix",
        "s.event_at_hour",
        "s.event_at_hour_minus_avg",
        "s.ticket_csat",
        "s.agent_tier",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    line_level_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_hour",
        "event_at_hour_minus_avg",
        "ticket_csat",
        "agent_tier",
        "event_id",
        "event_event_at",
        "event_event_type",
        F.greatest(F.col("ticket_csat"), F.col("event_at_hour_minus_avg")).alias("greatest_ticket_csat_event_at_hour_minus_avg"),
        F.substring(F.trim(F.col("actor_prefix")), 1, 3).alias("actor_prefix_prefix"),
    )
    .orderBy(
        F.col("event_at_hour_minus_avg").desc(),
        F.col("actor_prefix_prefix").desc(),
    )
)

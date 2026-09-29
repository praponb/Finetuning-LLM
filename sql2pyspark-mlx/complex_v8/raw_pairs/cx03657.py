from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.hour(F.col("event_at")).between(10, 16)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.when(F.col("event_type").isin("escalated", "reopened"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

without_matches = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("system", "customer")), "left_semi")
)

stats = (
    without_matches
    .groupBy(
        "ticket_id",
        F.year("event_at").alias("event_at_year"),
        "event_type_group",
    )
    .agg(
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("event_type") != "replied", 1).otherwise(0)).alias("n_event_type_replied"),
    )
    .filter(F.col("n_rows") >= 2)
)

joined = (
    stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.csat") > 2)
    .select(
        "s.ticket_id",
        "s.event_at_year",
        "s.event_type_group",
        "s.n_rows",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.ticket_id",
        "s.event_at_year",
        "s.event_type_group",
        "s.n_rows",
        "s.ticket_csat",
        "s.ticket_opened_at",
        "s.agent_hired_on",
        "s.agent_team",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    with_parent
    .groupBy("agent_team")
    .pivot("ticket_priority", ["P4", "P1", "P2"])
    .agg(F.avg("ticket_csat"))
)

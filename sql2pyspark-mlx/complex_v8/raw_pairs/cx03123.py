from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(~F.col("channel").isin("email", "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

combined_details_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on") < F.lit("2024-12-31").cast("date"))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    combined_details_agent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.last_event_at",
    )
)

channel_stats = (
    with_counts
    .groupBy("channel")
    .agg(F.max("event_actor_count").alias("max_event_actor_count"))
)

compared = (
    with_counts.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.event_actor_count") == F.col("g.max_event_actor_count"))
    .select(
        "s.*",
        "g.max_event_actor_count",
        F.round(F.col("s.event_actor_count") - F.col("g.max_event_actor_count"), 2).alias("event_actor_count_minus_max"),
    )
)

combined_details_agent_2 = (
    compared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.agent_hired_on",
        "s.event_actor_count",
        "s.event_actor_count_minus_max",
        "su.agent_name",
    )
)

result = (
    combined_details_agent_2
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "event_actor_count",
        "event_actor_count_minus_max",
        "agent_name",
        F.coalesce(F.col("event_actor_count"), F.lit(0)).alias("event_actor_count_filled"),
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
    .orderBy(F.col("event_actor_count_filled").desc(), F.col("agent_id").desc(), "id")
    .limit(20)
)

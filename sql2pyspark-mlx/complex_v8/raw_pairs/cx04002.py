from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(7, 9))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

combined_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_len",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

with_details_agent = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

joined = (
    with_details_agent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_len",
        "s.ticket_agent_id",
        "s.ticket_channel",
        "s.agent_team",
        "s.agent_hired_on",
        "s.agent_tier",
        "s.ticket_opened_at",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id_2"),
    )
)

filtered = (
    joined.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id_2")) & (F.col("t.csat").between(2, 4)), "left_anti")
    .filter(F.col("event_type").isin("replied", "assigned"))
)

agent_tier_stats = (
    filtered
    .groupBy("agent_tier")
    .agg(F.avg("event_type_len").alias("avg_event_type_len"))
)

compared = (
    filtered.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.event_type_len") > F.col("g.avg_event_type_len"))
    .select(
        "s.*",
        "g.avg_event_type_len",
        F.round(F.col("s.event_type_len") - F.col("g.avg_event_type_len"), 2).alias("event_type_len_minus_avg"),
    )
)

combined_details_2 = (
    compared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 6)
    .select(
        "s.*",
        "su.agent_name",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    combined_details_2
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_len",
        "ticket_agent_id",
        "ticket_channel",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        "ticket_agent_id_2",
        "event_type_len_minus_avg",
        "agent_name",
        "ticket_requester_id",
        F.quarter(F.col("agent_hired_on")).alias("agent_hired_on_quarter"),
    )
    .orderBy(F.col("agent_tier").desc(), F.col("ticket_id").desc(), "id")
    .limit(10)
)

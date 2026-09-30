from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter(F.col("team") == "Onboarding")
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
    )
)

exploded_detail_tickets = (
    valid_agents.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.hired_on",
        F.col("t.id").alias("ticket_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_children_tickets = (
    exploded_detail_tickets.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.ticket_channel",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel_2"),
    )
)

labeled = (
    with_children_tickets
    .select(
        "*",
        F.length(F.trim(F.col("ticket_channel"))).alias("ticket_channel_len"),
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"),
        F.max("requester_id").alias("max_ticket_requester_id"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        "t.avg_ticket_requester_id",
        "t.max_ticket_requester_id",
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id_2"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.ticket_id_2",
        "s.ticket_csat",
        "s.ticket_tags",
        "s.ticket_channel_2",
        "s.ticket_channel_len",
        "s.ticket_channel_slug",
        "s.avg_ticket_requester_id",
        "s.max_ticket_requester_id",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_csat",
        "ticket_tags",
        "ticket_channel_len",
        "ticket_channel_slug",
        "avg_ticket_requester_id",
        "ticket_opened_at",
        "ticket_agent_id",
        "ticket_requester_id",
        "agent_name",
    )
    .orderBy(F.col("avg_ticket_requester_id").asc_nulls_last(), "ticket_agent_id")
)

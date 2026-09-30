from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("csat").alias("total_ticket_csat"),
        F.count("*").alias("ticket_count"),
        F.max("csat").alias("max_ticket_csat"),
    )
)

with_counts = (
    valid_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.total_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_csat",
    )
)

combined_details_agent = (
    with_counts.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("K%"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.priority_clean",
        "s.closed_at_quarter",
        "s.csat_per_requester_id",
        "s.total_ticket_csat",
        "s.ticket_count",
        "su.agent_name",
    )
)

combined_details = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "priority_clean",
        "closed_at_quarter",
        "ticket_count",
        "agent_name",
        "agent_agent_name",
        "agent_team",
        F.size(F.col("tags")).alias("tags_count"),
        F.concat_ws(": ", F.col("agent_agent_name"), F.col("priority_clean")).alias("agent_agent_name_priority_clean_label"),
    )
    .orderBy(F.col("opened_at").asc(), F.col("agent_agent_name").asc(), "id")
    .limit(20)
)

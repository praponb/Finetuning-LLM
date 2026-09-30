from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("requester_id") > 6) & (F.col("priority").isin("P2", "P4", "P3")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    valid_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

enriched = (
    with_counts.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "VIP")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.csat_per_requester_id",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    enriched.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    with_stats
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "csat_per_requester_id",
        "agent_name",
        "agent_team",
        "last_event_at",
    )
    .orderBy(F.col("csat").asc(), "id")
    .limit(10)
)

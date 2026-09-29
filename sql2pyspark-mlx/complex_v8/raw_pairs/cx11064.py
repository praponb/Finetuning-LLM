from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.year(F.col("closed_at")).alias("closed_at_year"),
    )
)

joined = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

screened = (
    joined.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") > 1), "left_anti")
    .filter(F.size(F.col("tags")) >= 2)
)

tagged = (
    screened
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "closed_at_year",
        "agent_team",
        "agent_tier",
        "agent_name",
        F.abs(F.col("requester_id") - 8).alias("requester_id_distance"),
        F.coalesce(F.col("agent_tier"), F.lit("UNKNOWN")).alias("agent_tier_or_unknown"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

profiled = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    profiled
    .filter(F.lower(F.col("agent_name")).like("%s %"))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "closed_at_year",
        "agent_team",
        "agent_name",
        "requester_id_distance",
        "agent_tier_or_unknown",
        "requester_id_plus_csat",
        "first_event_at",
        F.substring(F.trim(F.col("agent_tier")), 1, 3).alias("agent_tier_prefix"),
    )
    .orderBy(F.col("channel").asc_nulls_last())
)

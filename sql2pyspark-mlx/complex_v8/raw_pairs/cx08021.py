from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
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
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isNotNull()), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.sum("csat").alias("total_ticket_csat"))
)

with_stats = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.total_ticket_csat")
)

scored = (
    with_stats
    .filter(~F.col("priority").isin("P4", "P3"))
    .select(
        "*",
        F.length(F.trim(F.col("priority_or_unknown"))).alias("priority_or_unknown_len"),
        F.concat(F.col("priority_ref"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref_ref"),
    )
)

enriched_agent = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.priority_or_unknown",
        "s.total_ticket_csat",
        "s.priority_or_unknown_len",
        "s.priority_ref_ref",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "priority_or_unknown",
        "total_ticket_csat",
        "priority_or_unknown_len",
        "priority_ref_ref",
        "agent_name",
        F.round(F.col("total_ticket_csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("total_ticket_csat_per_requester_id"),
    )
    .orderBy(F.col("priority").asc_nulls_last(), "priority_or_unknown_len")
)

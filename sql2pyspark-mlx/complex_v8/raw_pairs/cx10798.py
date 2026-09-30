from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) & (F.hour(F.col("opened_at")).between(9, 14)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

qualified = (
    recent_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")) & (F.col("t.csat") < 4), "left_anti")
)

with_details_agent = (
    qualified.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

exploded_detail_events = (
    with_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_len",
        "s.csat_per_requester_id",
        "s.agent_team",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    exploded_detail_events
    .cube("csat", "priority")
    .agg(
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.min("csat_per_requester_id").alias("min_csat_per_requester_id"),
        F.max("agent_hired_on").alias("last_agent_hired_on"),
    )
    .select(
        "csat",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "distinct_agent_teams",
        "min_csat_per_requester_id",
        "last_agent_hired_on",
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("priority").asc_nulls_last())
)

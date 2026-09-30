from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.col("priority") == "P2") | (F.col("requester_id") > 5))
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
        F.when(F.col("requester_id") >= 22, "A").when(F.col("requester_id") >= 6, "B").otherwise("C").alias("requester_id_tier"),
        F.when(F.col("requester_id") >= 21, "HIGH").when(F.col("requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("requester_id_tier_2"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

detail_rows = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.event_type").isin("replied", "assigned")), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_tier",
        "s.requester_id_tier_2",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

joined = (
    detail_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.requester_id_tier_2",
        "s.event_id",
        "s.event_at",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined
    .select(
        "id",
        "tags",
        "event_id",
        "event_at",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        F.abs(F.col("csat") - 3).alias("csat_distance"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
    .orderBy("agent_team", F.col("id").desc(), "event_id")
    .limit(20)
)

from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

with_parent_ticket = (
    covered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.closed_at")).isin(1, 2)), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details = (
    with_parent_ticket.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "s.ticket_requester_id",
        "s.agent_team",
        "s.agent_tier",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team_2"),
    )
)

result = (
    with_details
    .rollup("agent_team_2", "agent_team")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(100 * F.count(F.when(F.col("agent_tier") == "T2", 1)) / F.count("*"), 1).alias("pct_agent_tier_t2"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
    )
    .orderBy(
        F.col("agent_team_2").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)

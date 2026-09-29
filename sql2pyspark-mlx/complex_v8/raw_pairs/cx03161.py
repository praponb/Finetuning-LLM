from pyspark.sql import functions as F

in_window = (
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

combined_details_agent = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Onboarding"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

combined_details = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team_2"),
        F.col("su.tier").alias("agent_tier_2"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["agent_tier_2", "agent_team_2", "ticket_id"], ["agent_tier_2"]],
        "agent_tier_2", "agent_team_2", "ticket_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("agent_tier_2")).alias("agent_tier_2_list"),
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
    )
    .orderBy(
        F.col("agent_tier_2").asc_nulls_last(),
        F.col("agent_team_2").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)

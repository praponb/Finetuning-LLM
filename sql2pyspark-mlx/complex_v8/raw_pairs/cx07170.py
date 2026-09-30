from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("csat") > 3) | (F.col("agent_id") <= 2))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

filtered = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
    .filter(F.col("csat").between(1, 5))
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_priority_label",
        "s.minutes_opened_at_to_closed_at",
        "s.csat_tier",
        "s.agent_tier",
        "s.agent_team",
        "s.agent_name",
        F.col("su.team").alias("agent_team_2"),
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    with_details
    .rollup("agent_tier", "agent_tier_2", "csat_tier")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("minutes_opened_at_to_closed_at").alias("total_minutes_opened_at_to_closed_at"),
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_tier_2").asc_nulls_last(),
        F.col("csat_tier").asc_nulls_last(),
    )
)

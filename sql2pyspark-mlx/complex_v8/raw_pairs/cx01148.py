from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

with_parent_agent = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

labeled = (
    with_parent_agent
    .filter(F.col("agent_hired_on") >= F.lit("2024-05-02").cast("date"))
    .select(
        "id",
        "priority",
        "agent_name",
        "agent_team",
        F.concat_ws(" / ", F.col("agent_name"), F.col("agent_team")).alias("agent_name_agent_team_label"),
        F.month(F.col("agent_hired_on")).alias("agent_hired_on_month"),
    )
)

result = (
    labeled
    .rollup("priority", "agent_team")
    .agg(
        F.grouping("priority").alias("is_total_priority"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.sum("agent_hired_on_month").alias("total_agent_hired_on_month"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        "is_total_priority",
        "distinct_prioritys",
        "total_agent_hired_on_month",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)

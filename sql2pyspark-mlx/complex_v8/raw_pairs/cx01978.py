from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
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

totals = (
    covered
    .groupBy("event_type", "agent_id", "priority")
    .agg(
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.max("event_at").alias("last_event_at"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("reopened", "escalated"), 1)) / F.count("*"), 1).alias("pct_event_type_in_reopened_escal"),
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("hours_since_open").alias("max_hours_since_open_check"),
    )
    .filter(F.col("max_hours_since_open_check") > 7.2)
    .drop("max_hours_since_open_check")
)

with_details_agent = (
    totals.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.event_type",
        "s.agent_id",
        "s.priority",
        "s.avg_hours_since_open",
        "s.last_event_at",
        "s.min_hours_since_open",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details_agent
    .select(
        "event_type",
        "agent_id",
        "priority",
        "last_event_at",
        "min_hours_since_open",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
    )
    .orderBy(
        F.col("agent_hired_on").desc_nulls_first(),
        F.col("agent_team").desc_nulls_first(),
    )
)

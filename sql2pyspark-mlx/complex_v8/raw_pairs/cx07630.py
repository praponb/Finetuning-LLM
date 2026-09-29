from pyspark.sql import functions as F

covered = (
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

summary = (
    covered
    .groupBy("agent_id", "priority")
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.sum("hours_since_open").alias("total_hours_since_open"),
        F.count("*").alias("row_count"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .filter(F.col("row_count") >= 2)
)

enriched_agent = (
    summary.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched_agent
    .select(
        "agent_id",
        "priority",
        "priority_list",
        "stddev_hours_since_open",
        "total_hours_since_open",
        "distinct_prioritys",
        "agent_hired_on",
        F.date_trunc("year", F.col("agent_hired_on")).alias("agent_hired_on_year"),
    )
    .orderBy("agent_hired_on_year", F.col("stddev_hours_since_open").asc())
)

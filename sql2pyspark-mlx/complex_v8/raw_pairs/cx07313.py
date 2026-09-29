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

joined_agent = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("closed_at").alias("last_ticket_closed_at_2"),
    )
)

with_stats = (
    joined_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_closed_at", "t.last_ticket_closed_at_2")
)

per_group = (
    with_stats
    .groupBy("agent_team")
    .agg(
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("priority").isNotNull(), 1).otherwise(0)).alias("n_priority_is_not_null"),
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
)

result = (
    per_group
    .select(
        "agent_team",
        "agent_name_list",
        "row_count",
        "stddev_hours_since_open",
        "max_hours_since_open",
        "distinct_prioritys",
        (F.floor(F.col("distinct_prioritys") / 2) * 2).alias("distinct_prioritys_bucket"),
        F.when(F.col("max_hours_since_open") >= 50.4, "A").when(F.col("max_hours_since_open") >= 28.8, "B").otherwise("C").alias("max_hours_since_open_tier"),
    )
    .orderBy("agent_team", F.col("row_count").asc_nulls_last())
)

from pyspark.sql import functions as F

matched = (
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

per_group = (
    matched
    .groupBy("priority", "agent_id", F.to_date("event_at").alias("event_at_day"))
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.sum(F.when(F.col("priority") != "P1", F.col("hours_since_open")).otherwise(0)).alias("hours_since_open_priority_p1"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
)

scored = (
    per_group
    .filter(F.size(F.col("priority_list")) >= 2)
    .select(
        "priority",
        "agent_id",
        "event_at_day",
        "distinct_ids",
        "total_hours_since_open",
        "priority_list",
        F.least(F.col("total_hours_since_open"), F.col("distinct_ids")).alias("least_total_hours_since_open_distinct_ids"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count(F.when(~F.col("channel").isin("chat", "email"), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
    )
)

with_counts = (
    scored.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_counts
    .select(
        "priority",
        "agent_id",
        "event_at_day",
        "distinct_ids",
        "total_hours_since_open",
        "priority_list",
        "max_ticket_requester_id",
        "ticket_channel_count",
        "ticket_count",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(F.col("agent_id").desc_nulls_first())
)

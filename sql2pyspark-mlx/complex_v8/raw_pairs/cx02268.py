from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
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
        F.abs(F.col("csat") - 3).alias("csat_distance"),
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

line_level = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.csat_distance",
        "s.closed_at_plus_15m",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

with_counts = (
    line_level.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_opened_at",
    )
)

rollup_base = (
    with_counts
    .groupBy(
        F.date_trunc("month", "first_ticket_opened_at").alias("first_ticket_opened_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("closed_at_plus_15m").alias("last_closed_at_plus_15m"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.max("requester_id").alias("max_requester_id"),
    )
)

result = (
    rollup_base
    .select(
        "first_ticket_opened_at_month",
        "priority_list",
        "last_closed_at_plus_15m",
        "channel_list",
        "distinct_ids",
        "max_requester_id",
        (F.col("last_closed_at_plus_15m") + F.expr("INTERVAL 2 HOURS")).alias("last_closed_at_plus_15m_plus_2h"),
        F.year(F.col("last_closed_at_plus_15m")).alias("last_closed_at_plus_15m_year"),
    )
    .orderBy(
        F.col("last_closed_at_plus_15m").desc_nulls_first(),
        "last_closed_at_plus_15m_plus_2h",
    )
)

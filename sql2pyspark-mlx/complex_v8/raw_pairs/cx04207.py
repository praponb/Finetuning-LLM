from pyspark.sql import functions as F

joined_ranges = (
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

tagged = (
    joined_ranges
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix_2"),
    )
)

stats = (
    tagged
    .groupBy("event_type")
    .agg(
        F.sum("hours_since_open").alias("total_hours_since_open"),
        F.min("hours_since_open").alias("min_hours_since_open"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("total_hours_since_open").alias("overall_avg_total_hours_since_open")))
    .filter(F.col("total_hours_since_open") > F.col("overall_avg_total_hours_since_open"))
    .drop("overall_avg_total_hours_since_open")
)

result = (
    filtered
    .select("event_type", "total_hours_since_open")
    .orderBy(F.col("event_type").asc())
    .limit(20)
)

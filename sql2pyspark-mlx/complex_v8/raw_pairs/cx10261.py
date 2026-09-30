from pyspark.sql import functions as F

in_window = (
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

event_type_stats = (
    in_window
    .groupBy("event_type")
    .agg(F.avg("hours_since_open").alias("avg_hours_since_open"))
)

outliers = (
    in_window.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.hours_since_open") > F.col("g.avg_hours_since_open"))
    .select(
        "s.*",
        "g.avg_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.avg_hours_since_open"), 2).alias("hours_since_open_minus_avg"),
    )
)

scored = (
    outliers
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "event_type",
        "priority",
        "avg_hours_since_open",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

per_group = (
    scored
    .filter(F.col("event_type") == "reopened")
    .groupBy("event_type", "priority")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.max("avg_hours_since_open").alias("max_avg_hours_since_open"),
    )
)

result = (
    per_group
    .groupBy("priority")
    .pivot("event_type", ["created", "replied", "escalated"])
    .agg(F.min("max_avg_hours_since_open"))
    .orderBy("priority")
)

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

priority_stats = (
    joined_ranges
    .groupBy("priority")
    .agg(F.avg("hours_since_open").alias("avg_hours_since_open"))
)

compared = (
    joined_ranges.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.hours_since_open") > F.col("g.avg_hours_since_open"))
    .select(
        "s.*",
        "g.avg_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.avg_hours_since_open"), 2).alias("hours_since_open_minus_avg"),
    )
)

result = (
    compared
    .filter(F.col("avg_hours_since_open") > 25.2)
    .select(
        "id",
        "event_at",
        "agent_id",
        "hours_since_open",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.greatest(F.col("avg_hours_since_open"), F.col("hours_since_open")).alias("greatest_avg_hours_since_open_hours_since_open"),
    )
)

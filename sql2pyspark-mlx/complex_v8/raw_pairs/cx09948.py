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

summary = (
    in_window
    .groupBy("priority", "agent_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.min("event_at").alias("first_event_at"),
        F.sum(F.when(F.col("event_type") == "resolved", F.col("hours_since_open"))).alias("hours_since_open_event_type_resolved"),
        F.count(F.when(F.col("priority") != "P3", 1)).alias("n_priority_p3"),
    )
)

result = (
    summary
    .select(
        "priority",
        "agent_id",
        "median_hours_since_open",
        "first_event_at",
        "hours_since_open_event_type_resolved",
        "n_priority_p3",
        F.abs(F.col("n_rows") - 5).alias("n_rows_distance"),
    )
    .orderBy(F.col("first_event_at").desc())
)

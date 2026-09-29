from pyspark.sql import functions as F

in_window = (
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

totals = (
    in_window
    .groupBy("event_type", "agent_id", "ticket_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("hours_since_open").alias("max_hours_since_open_check"),
    )
    .filter(F.col("max_hours_since_open_check") > 21.6)
    .drop("max_hours_since_open_check")
)

result = (
    totals
    .select(
        "event_type",
        "agent_id",
        "ticket_id",
        "n_rows",
        "event_type_list",
        "stddev_hours_since_open",
        "priority_list",
        F.abs(F.col("stddev_hours_since_open") - 43.2).alias("stddev_hours_since_open_distance"),
    )
    .orderBy(F.col("agent_id").desc(), F.col("ticket_id").desc())
)

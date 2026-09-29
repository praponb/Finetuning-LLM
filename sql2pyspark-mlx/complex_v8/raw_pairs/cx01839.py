from pyspark.sql import functions as F

in_window = (
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

labeled = (
    in_window
    .select(
        "id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    labeled.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "event_type",
        "priority",
        "hours_since_open",
        "priority_or_unknown",
        "event_type_ref",
        "priority_len",
    )
    .orderBy(F.col("priority").asc_nulls_last())
)

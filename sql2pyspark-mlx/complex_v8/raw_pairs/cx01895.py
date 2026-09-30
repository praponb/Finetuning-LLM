from pyspark.sql import functions as F

covered = (
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

ticket_id_stats = (
    covered
    .groupBy("ticket_id")
    .agg(F.max("hours_since_open").alias("max_hours_since_open"))
)

outliers = (
    covered.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.hours_since_open") == F.col("g.max_hours_since_open"))
    .select(
        "s.*",
        "g.max_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.max_hours_since_open"), 2).alias("hours_since_open_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "hours_since_open",
        "hours_since_open_minus_max",
    )
    .orderBy("id", F.col("hours_since_open_minus_max").desc_nulls_first())
)

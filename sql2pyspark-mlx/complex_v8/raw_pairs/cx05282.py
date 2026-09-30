from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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
    covered
    .groupBy("event_type")
    .agg(F.avg("hours_since_open").alias("avg_hours_since_open"))
)

outliers = (
    covered.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.hours_since_open") > F.col("g.avg_hours_since_open"))
    .select(
        "s.*",
        "g.avg_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.avg_hours_since_open"), 2).alias("hours_since_open_minus_avg"),
    )
)

exploded_detail = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "s.avg_hours_since_open",
        "s.hours_since_open_minus_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "ticket_id",
        "event_at",
        "agent_id",
        "hours_since_open",
        "avg_hours_since_open",
        "hours_since_open_minus_avg",
        "event_id",
        "event_actor",
        "event_event_at",
        "event_event_type",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
    .orderBy(F.col("event_at_quarter").desc())
)
